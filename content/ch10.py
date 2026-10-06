ORDER = 10
KEY = '10'
TITLE = 'Performance and Internals: plans, shuffles, AQE, caching, skew'
WEEK = 'Week 5'
DATA = ['data/perf/sales (Parquet, 1M rows)', 'data/perf/stores']

SETUP_TITLE = '10.0 The data — a million sales rows in Parquet'
SETUP = '''import os
from pyspark.sql import functions as F
from pyspark import StorageLevel

(spark.range(1_000_000)
      .select(F.col("id").alias("sale_id"),
              (F.col("id") % 500).alias("store_id"),
              F.expr("date_add('2026-01-01', cast(id % 90 as int))").alias("day"),
              (F.rand(seed=42) * 1000).cast("int").alias("amount"))
      .write.mode("overwrite").parquet("data/perf/sales"))

(spark.range(500)
      .select(F.col("id").alias("store_id"),
              F.element_at(F.array(*[F.lit(c) for c in ["Pune", "Delhi", "Mumbai", "Chennai"]]),
                           (F.col("id") % 4 + 1).cast("int")).alias("city"))
      .write.mode("overwrite").parquet("data/perf/stores"))

sales = spark.read.parquet("data/perf/sales")
stores = spark.read.parquet("data/perf/stores")
print(sales.count(), stores.count())'''
SETUP_EXPL = '''One million sales across 500 stores and 90 days, plus a small `stores` dimension, both written to Parquet so Spark knows their sizes — the realistic starting point for tuning.

Timings are deliberately not shown in these notes: they depend on the laptop. Everything here is read from **plans and partition counts**, which are reproducible.'''

SECTIONS = [
    dict(t='10.1 Read a real plan: explain(mode="formatted")',
         code='''report = (sales.filter(F.col("amount") > 500)
               .join(stores, "store_id")
               .groupBy("city")
               .agg(F.sum("amount").alias("revenue")))
report.explain(mode="formatted")''',
         expl='''`formatted` prints a short operator tree first, then the details of each numbered node underneath — far easier to read than the default one-line form.

Find, from the bottom: two `Scan parquet` nodes with pushed filters, the `BroadcastHashJoin` (stores is small), partial and final `HashAggregate` around one `Exchange` — exactly **one shuffle** for the whole report.'''),

    dict(t='10.2 How many partitions after a shuffle? (spark.sql.shuffle.partitions)',
         code='''print(spark.conf.get("spark.sql.shuffle.partitions"))
print(spark.conf.get("spark.sql.adaptive.enabled"))

spark.conf.set("spark.sql.adaptive.enabled", False)
print("AQE off:", sales.groupBy("store_id").count().rdd.getNumPartitions())

spark.conf.set("spark.sql.adaptive.enabled", True)
print("AQE on: ", sales.groupBy("store_id").count().rdd.getNumPartitions())''',
         expl='''After every shuffle Spark uses `spark.sql.shuffle.partitions` partitions — **200** by default, whatever the data size. Without AQE, 500 small groups are spread over 200 tiny partitions = 200 tasks.

With **Adaptive Query Execution** (on by default since Spark 3.2) Spark measures the shuffle output at run time and **coalesces** small partitions — here into a single one, because the whole result is tiny.'''),

    dict(t='10.3 AQE rewrites the plan while the query runs',
         code='''by_store = sales.groupBy("store_id").agg(F.sum("amount").alias("revenue"))
by_store.collect()          # run it, so AQE can finalise the plan
by_store.explain()''',
         expl='''Before running, the plan says `isFinalPlan=false`. After an action, `explain` shows the **Final Plan** next to the **Initial Plan**. The `AQEShuffleRead coalesced` node is where AQE merged the 200 shuffle partitions.

AQE does three things at run time: **coalesces** small shuffle partitions, **switches join strategy** (sort-merge → broadcast) when one side turns out small, and **splits skewed partitions** in joins.'''),

    dict(t='10.4 repartition vs coalesce in the plan',
         code='''sales.repartition(8).explain()
sales.coalesce(2).explain()
print(sales.rdd.getNumPartitions(), sales.coalesce(2).rdd.getNumPartitions())''',
         expl='''`repartition(8)` adds an `Exchange RoundRobinPartitioning(8)` — a full shuffle that produces evenly sized partitions. `coalesce(2)` shows a `Coalesce 2` node and **no Exchange**: it glues existing partitions together on the same executors.

Use `coalesce` only to **reduce** partitions cheaply (before writing); use `repartition` to increase them or to fix uneven ones.'''),

    dict(t='10.5 Repartition by a column before a heavy operation',
         code='''by_day = sales.repartition(4, "day")
by_day.explain()

(by_day.withColumn("p", F.spark_partition_id())
       .groupBy("p").agg(F.countDistinct("day").alias("days"), F.count("*").alias("rows"))
       .orderBy("p").show())''',
         expl='''`repartition(n, col)` hash-partitions: all rows of one `day` land in the same partition (`Exchange hashpartitioning(day, 4)`). A later `groupBy("day")` or a write with `partitionBy("day")` then needs no further shuffle.

The row counts per partition are uneven — hashing 90 days into 4 buckets is not perfectly balanced.'''),

    dict(t='10.6 Reuse a result: cache() and the InMemoryRelation',
         code='''hot = sales.filter(F.col("amount") > 900).cache()
print(hot.storageLevel)
print(hot.count())                       # first action fills the cache
hot.groupBy("store_id").count().explain()''',
         expl='''`cache()` marks the DataFrame to be kept in executor memory (spilling to disk if needed: `MEMORY_AND_DISK`). It is **lazy** — the first action fills it. After that the plan reads from `InMemoryTableScan` instead of the Parquet files.

Cache only what you reuse **several times** in the same job (an iterative loop, several reports from one cleaned table). Caching something used once only adds work.'''),

    dict(t='10.7 Choose where to keep it: persist(StorageLevel) and unpersist',
         code='''hot.unpersist()
disk_copy = sales.select("store_id", "amount").persist(StorageLevel.DISK_ONLY)
disk_copy.count()
print(disk_copy.storageLevel)
print(disk_copy.is_cached)
disk_copy.unpersist()
print(disk_copy.is_cached)''',
         expl='''`persist(level)` chooses the storage: `MEMORY_ONLY`, `MEMORY_AND_DISK` (what `cache()` uses for DataFrames), `DISK_ONLY`, or `_2` variants that keep two copies.

Always `unpersist()` when done — cached blocks occupy executor memory that joins and aggregations need.'''),

    dict(t='10.8 Spot data skew: one key owns most of the rows',
         code='''skewed = spark.range(200_000).withColumn(
    "store_id", F.when(F.col("id") % 10 < 9, F.lit(7)).otherwise(F.col("id") % 100))

(skewed.repartition(8, "store_id")
       .withColumn("p", F.spark_partition_id())
       .groupBy("p").count().orderBy(F.desc("count")).show())''',
         expl='''90% of the rows belong to store 7. Hash-partitioning by `store_id` sends **all** of them to one partition: one task processes 180,000+ rows while the others handle a few thousand.

In the Spark UI this shows up as one task running far longer than the median in a stage — the classic "99% done, waiting for the last task" job.'''),

    dict(t='10.9 Fix skew in an aggregation: salting',
         code='''salted = (skewed
    .withColumn("salt", (F.rand(seed=1) * 8).cast("int"))
    .groupBy("store_id", "salt").agg(F.count("*").alias("part_count")))   # step 1: spread the hot key

totals = salted.groupBy("store_id").agg(F.sum("part_count").alias("rows"))  # step 2: combine
totals.orderBy(F.desc("rows")).show(3)

(skewed.withColumn("salt", (F.rand(seed=1) * 8).cast("int"))
       .repartition(8, "store_id", "salt")
       .groupBy(F.spark_partition_id().alias("p")).count().orderBy("p").show())''',
         expl='''Add a random **salt** (0–7) and group by `(store_id, salt)` first: the hot key is split across 8 groups and 8 tasks. A second, tiny aggregation adds the 8 partial counts back together. The result is identical; the work is spread out.

The last table shows the partitions after salting: the hot key is spread over all 8 — the largest now holds about 49,000 rows instead of 180,000. Hash collisions keep it from being perfectly even. For **joins**, AQE's skew-join handling (`spark.sql.adaptive.skewJoin.enabled`, on by default) splits oversized partitions automatically.'''),

    dict(t='10.10 Join strategy hints',
         code='''small_sales = sales.limit(1000)
small_sales.join(stores.hint("shuffle_hash"), "store_id").explain()''',
         expl='''Hints override Spark's choice of join: `broadcast`, `merge` (sort-merge), `shuffle_hash`, `shuffle_replicate_nl`. Here `shuffle_hash` builds a hash table from the shuffled `stores` side instead of sorting both sides.

Use hints when you know something the optimiser cannot: missing statistics, or a side that is small after filtering.'''),

    dict(t='10.11 Built-in filters are pushed down; Python filters are not',
         code='''from pyspark.sql.functions import udf

is_big = udf(lambda a: a is not None and a > 990, "boolean")

sales.filter(F.col("amount") > 990).explain()
sales.filter(is_big("amount")).explain()''',
         expl='''With a built-in comparison, `PushedFilters: [IsNotNull(amount), GreaterThan(amount,990)]` — Parquet skips row groups that cannot match. With a Python UDF, `PushedFilters: []`: every row is read, sent to a Python worker (`ArrowEvalPython` — Spark 4.2 batches regular UDFs through Arrow) and back.

The same logic, written two ways, can differ by an order of magnitude. Chapter 11 covers UDFs properly.'''),

    dict(t='10.12 Check what the session is using',
         code='''for k in ["spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
          "spark.sql.adaptive.coalescePartitions.enabled", "spark.sql.adaptive.skewJoin.enabled",
          "spark.sql.autoBroadcastJoinThreshold", "spark.sql.files.maxPartitionBytes"]:
    print(f"{k:50} {spark.conf.get(k)}")''',
         expl='''These six settings explain most tuning conversations. `maxPartitionBytes` (128 MB) decides how many partitions a file read produces; `autoBroadcastJoinThreshold` is in bytes (10 MB).

Set them per job with `spark.conf.set(...)` or at submit time with `--conf key=value`.'''),
]

POINTS = '''
1. Read plans bottom-up; count **Exchange** nodes (shuffles) first — they dominate cost.
2. `spark.sql.shuffle.partitions` = 200 by default; **AQE** coalesces small shuffle partitions, switches to broadcast joins and splits skewed partitions at run time.
3. `repartition(n[, col])` = full shuffle (balance, or co-locate a key); `coalesce(n)` = merge without shuffle (only to fewer).
4. Join strategies: **BroadcastHashJoin** (small side ≤ 10 MB, no shuffle of the big side) → **SortMergeJoin** (two shuffles + sorts, any size). Hint with `F.broadcast(df)` or `df.hint("…")`.
5. `cache()` / `persist(level)` are lazy; cache only reused results and `unpersist()` afterwards.
6. **Skew** = one partition much bigger than the rest. Fix with salting (aggregations) or AQE skew join (joins); isolate hot keys.
<!--break-->
7. Keep work in the JVM: built-in functions get pushdown and code generation; Python UDFs get neither.

    | Symptom in the Spark UI | Likely cause | First fix |
    |---|---|---|
    | One task much slower than the rest | Skew | Salt the key / AQE skew join |
    | Thousands of tiny tasks | Too many partitions / small files | AQE coalescing, `coalesce` before write |
    | Spill to disk, OOM in a stage | Partitions too large | More shuffle partitions, `repartition` |
    | Big shuffle on a join with a small table | No broadcast | `F.broadcast(small)` |
    | Same stages computed twice | Reused result not cached | `cache()` then `unpersist()` |
'''

EXTRA_TITLE = 'Deep dive: memory and cluster sizing'
EXTRA_LEAD = 'The vocabulary behind "executor lost" and "container killed by YARN".'
EXTRA = [
    dict(t='Inside an executor\'s memory',
         md='''| Region | Default share | Holds |
|---|---|---|
| **Reserved** | 300 MB | Spark internals |
| **Unified memory** (`spark.memory.fraction` = 0.6 of the rest) | 60% | **Execution** (shuffles, joins, sorts, aggregations) and **storage** (cached data) — they borrow from each other |
| **User memory** | 40% | Your objects, UDF data structures |
| **Overhead** (`spark.executor.memoryOverhead`) | max(384 MB, 10%) | Off-heap, Python workers, native buffers |''',
         expl='''When execution needs more memory it can evict cached blocks; when neither fits, Spark **spills** to disk (slow but safe). "Container killed for exceeding memory limits" usually means the **overhead** was too small — common with Python UDFs and pandas, which live outside the JVM heap.'''),
    dict(t='Sizing executors: a worked example',
         md='''| Step | Rule of thumb | 10 nodes × 16 cores × 64 GB |
|---|---|---|
| Cores per executor | 4–5 (HDFS/S3 throughput, GC) | 5 |
| Executors per node | (cores − 1 for OS) / 5 | 3 |
| Total executors | nodes × per node − 1 for the driver | 29 |
| Memory per executor | (RAM − 1 GB) / executors per node, minus ~10% overhead | ~19 GB heap |
| Parallelism | total cores × 2–3 | ~300–450 shuffle partitions |''',
         expl='''These are starting points, not laws. Target shuffle partitions of roughly **100–200 MB** each, then let AQE coalesce. On Databricks or EMR Serverless much of this is managed for you, but the vocabulary is the same in every interview.'''),
    dict(t='A tuning checklist, in order',
         md='''1. **Read less**: Parquet/Delta, select only needed columns, filter early on partition columns.
2. **Shuffle less**: broadcast small tables, avoid unnecessary `distinct` / `orderBy`, aggregate before joining.
3. **Balance the work**: check task-time spread in the Spark UI; salt or let AQE split skew.
4. **Right-size partitions**: ~128–200 MB; AQE coalescing on; `coalesce` before writing small outputs.
5. **Stay in the JVM**: built-ins and higher-order functions over UDFs; pandas UDFs when Python is unavoidable.
6. **Cache deliberately** and unpersist.
7. Only then: executor memory, cores and cluster size.'''),
]

MCQS = [
    dict(q='A stage has 200 tasks; 199 finish in 10 s and one takes 20 minutes. What is the most likely cause?', kind='concept',
         o=['Too few executors', 'Data skew — one partition holds far more rows', 'The driver is out of memory', 'Parquet compression'], a=1,
         why='Very uneven task durations in one stage are the signature of skew.'),
    dict(q='What does this print?', kind='output', run=True, check='3',
         code='''df = spark.range(100).repartition(6)
print(df.coalesce(3).rdd.getNumPartitions())''',
         o=['6', '3', '100', '200'], a=1,
         why='coalesce(3) merges the 6 partitions into 3 without a shuffle.'),
    dict(q='Which operation reduces the number of partitions **without** a shuffle?', kind='concept',
         o=['`repartition(2)`', '`coalesce(2)`', '`orderBy`', '`distinct`'], a=1,
         why='coalesce merges neighbouring partitions; repartition always shuffles.'),
    dict(q='`df.cache()` is called. When is the data actually cached?', kind='concept',
         o=['Immediately', 'At the first action that computes df', 'At the end of the application', 'Only after unpersist()'], a=1,
         why='cache is lazy: it marks the plan, and the first action materialises the cached blocks.'),
    dict(q='What is the default value of `spark.sql.shuffle.partitions`?', kind='concept',
         o=['8', '64', '200', 'The number of cores'], a=2,
         why='200, regardless of data size — AQE coalescing then adjusts it at run time.'),
    dict(q='Which plan line proves a filter was pushed into the Parquet reader?', kind='concept',
         o=['`Exchange hashpartitioning`', '`PushedFilters: [GreaterThan(amount,990)]`', '`AdaptiveSparkPlan isFinalPlan=false`', '`HashAggregate`'], a=1,
         why='PushedFilters lists conditions handed to the data source, which can skip row groups using statistics.'),
    dict(q='This aggregation is slow because 90% of rows have `country = "IN"`. Which change spreads the work?', kind='debug',
         code='''df.groupBy("country").agg(F.sum("amount"))''',
         o=['Add `.cache()` before groupBy', 'Group by (country, a random salt) first, then sum the partial results by country', 'Use `coalesce(1)`', 'Use `orderBy("country")` first'], a=1,
         why='Salting splits the hot key across several groups and tasks; a second small aggregation recombines them.'),
    dict(q='What does AQE\'s "coalesce partitions" feature do?', kind='concept',
         o=['Merges small shuffle partitions at run time based on their actual size', 'Deletes empty files', 'Caches every DataFrame', 'Turns off shuffles'], a=0,
         why='After a shuffle, AQE looks at real partition sizes and combines small ones to avoid many tiny tasks.'),
]

PRACTICE = dict(
    kinds=['Take three of your earlier solutions and explain() each: count shuffles, find the join strategy, look for PushedFilters.',
           'Generate a skewed dataset, watch the slow task in the Spark UI (localhost:4040), then fix it with salting.',
           'Compare partition counts with AQE on and off for the same groupBy.',
           'Cache a cleaned table used by three reports, then check the Storage tab of the Spark UI.'],
    note='Performance skills are practised on bigger data. Use spark.range() to generate 10–100 million rows on your laptop.')
