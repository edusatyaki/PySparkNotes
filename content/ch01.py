ORDER = 1
KEY = '1'
TITLE = 'Getting Started: SparkSession, partitions and lazy evaluation in code'
WEEK = 'Week 1'
DATA = ['students']

SETUP_TITLE = '1.0 Start Spark and build the first DataFrame'
SETUP = '''from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .master("local[4]")          # run on this laptop with 4 cores
         .appName("class-notes")
         .getOrCreate())

students = spark.createDataFrame(
    [(1, "Amit", "CSE", "Pune", 85), (2, "Riya", "ECE", "Mumbai", 72),
     (3, "Arjun", "CSE", "Delhi", 91), (4, "Sneha", "ME", "Pune", 67),
     (5, "Ankit", "CSE", "Kolkata", 88), (6, "Asha", "ECE", "Delhi", 76),
     (7, "Neha", "ECE", "Pune", 79), (8, "Rahul", "CSE", "Mumbai", 65),
     (9, "Priya", "ME", "Delhi", 82), (10, "Vikram", "CSE", "Jaipur", 94)],
    ["id", "name", "branch", "city", "marks"])'''
SETUP_EXPL = '''`SparkSession` is the single entry point to Spark. `builder ... getOrCreate()` returns the running session if one exists, otherwise it starts one. `local[4]` means "no cluster — use 4 cores of this machine".

`createDataFrame` turns a Python list of tuples into a distributed **DataFrame**. The second argument names the columns; Spark infers the types from the values.

`F` is the conventional alias for `pyspark.sql.functions` — almost every column function lives there.'''
SETUP_TIP = 'Nothing is computed yet. Creating a DataFrame only records *what* the data is; Spark does work only when you ask for a result.'

SECTIONS = [
    dict(t='1.1 Which Spark did getOrCreate() give you?',
         code='''print(spark.version)
print(spark.sparkContext.master)
print(spark.sparkContext.appName)
print(spark.sparkContext.defaultParallelism)''',
         expl='''`spark.version` is the Spark release. `master` shows where the work runs: `local[4]` = 4 worker threads on this machine (on a cluster you would see `yarn`, `k8s://…` or `spark://…`).

`defaultParallelism` is how many tasks Spark runs at the same time by default — here one per core.'''),

    dict(t='1.2 Look at the data', tables=['students'], home=True,
         code='''students.show()''',
         expl='''`show()` is an **action**: it runs the job and prints the first 20 rows as a text grid. Use `show(5)` for fewer rows and `show(truncate=False)` to see long strings in full.

The output is a picture of the data on the driver — it is not a pandas table and you cannot edit it.'''),

    dict(t='1.3 What types did Spark infer? (the schema)', tables=['students'],
         code='''students.printSchema()
print(students.columns)
print(students.dtypes)''',
         expl='''Every DataFrame has a **schema**: column names, types and whether NULL is allowed. Python `int` became `long` (64-bit) and `str` became `string`.

`columns` is a plain Python list of names; `dtypes` pairs each name with its type. Both are free — they read the schema, they do not run a job.'''),

    dict(t='1.4 Ask for the toppers — and notice that nothing runs', tables=['students'],
         code='''toppers = students.filter(F.col("marks") > 80)

print(type(toppers))
print(toppers)''',
         expl='''`filter` is a **transformation**. It returns a new DataFrame immediately, but no rows were read and no job started — `print(toppers)` shows only the schema.

This is **lazy evaluation**: Spark keeps adding steps to a plan and waits until an action needs a result. That lets it optimise the whole chain before running anything.''',
         tip='Transformations return a DataFrame. Actions return a value (a number, a list, printed rows) — that is the quickest way to tell them apart.'),

    dict(t='1.5 Now run it: actions trigger the job', tables=['students'],
         code='''print(toppers.count())
toppers.show()''',
         expl='''`count()` and `show()` are **actions**. Each one makes Spark turn the plan into a job, run it on the executors and send the result back to the driver.

Two actions = two jobs. Spark does not remember the result of the first one unless you `cache()` it (Chapter 10).'''),

    dict(t='1.6 Pull rows back into Python: collect, take, first', tables=['students'],
         code='''rows = toppers.collect()        # every row → a Python list
print(rows[0])
print(toppers.take(2))           # only the first 2 rows
first = students.first()
print(first.name, first["marks"], first.asDict())''',
         expl='''Each row comes back as a `Row`, which works like a named tuple: `r.name`, `r["marks"]`, or `r.asDict()`.

`collect()` brings **every** row to the driver. On a 10-row table that is fine; on a 1-billion-row table it crashes the driver. Prefer `take(n)`, `show()` or writing the result to files.'''),

    dict(t='1.7 How is the data split? (partitions)',
         code='''print(students.rdd.getNumPartitions())

big = spark.range(1_000_000)          # one column "id", 0 … 999999
print(big.rdd.getNumPartitions())
big.show(3)''',
         expl='''A DataFrame is split into **partitions** — chunks of rows. One task processes one partition, so the number of partitions is the maximum parallelism of a stage.

`spark.range(n)` builds a DataFrame of numbers without any input file; it is the quickest way to make big test data.'''),

    dict(t='1.8 Which partition does each row live in?',
         code='''by_part = (spark.range(20)
           .repartition(4)
           .withColumn("partition", F.spark_partition_id()))

by_part.groupBy("partition").count().orderBy("partition").show()''',
         expl='''`repartition(4)` reshuffles the rows into 4 partitions, and `spark_partition_id()` tags each row with the partition it sits in.

The counts are roughly equal: `repartition` spreads rows round-robin. Uneven partitions (**skew**) are a major cause of slow jobs — Chapter 10 shows how to spot and fix them.'''),

    dict(t='1.9 Read the plan before it runs (explain)', tables=['students'],
         code='''toppers.select("name", "marks").explain()''',
         expl='''`explain()` prints the **physical plan** — what Spark will actually do. Read it bottom-up: scan the in-memory rows (`Scan ExistingRDD`), apply the `Filter`, then `Project` (select) the two columns.

The `*(1)` prefix means these steps are fused into one piece of generated Java code (whole-stage code generation). No `Exchange` line = no shuffle.'''),

    dict(t='1.10 A wide transformation adds a shuffle', tables=['students'],
         code='''students.groupBy("branch").count().explain()''',
         expl='''`groupBy` needs all rows of one branch on the same executor, so Spark must move data across the network. That move is the **`Exchange hashpartitioning(branch, …)`** line — a **shuffle**.

Spark aggregates twice: a partial `HashAggregate` before the shuffle (fewer rows to send) and a final one after it. Every `Exchange` splits the job into a new **stage**. The `200` is `spark.sql.shuffle.partitions`, the default number of partitions after a shuffle; `AdaptiveSparkPlan` means Adaptive Query Execution may shrink it at run time.''',
         tip='Narrow transformations (filter, select, withColumn) keep each partition where it is. Wide ones (groupBy, join, orderBy, distinct, repartition) shuffle. Count the Exchange lines to count the shuffles.'),

    dict(t='1.11 The same thing with the RDD API (for context)',
         code='''sc = spark.sparkContext
words = sc.parallelize(["spark is fast", "spark is lazy", "pandas is not"])

counts = (words.flatMap(lambda line: line.split())
               .map(lambda w: (w, 1))
               .reduceByKey(lambda a, b: a + b))

print(sorted(counts.collect()))''',
         expl='''The **RDD** (Resilient Distributed Dataset) is Spark's original low-level API: a distributed collection of Python objects transformed with lambdas. The classic word count is `flatMap` → `map` → `reduceByKey`.

In PySpark you should use DataFrames instead: Spark cannot look inside a Python lambda to optimise it, and every row must be moved between the JVM and Python. Learn the RDD only to read old code and to understand the model.'''),

    dict(t='1.12 Move between pandas and Spark', tables=['students'],
         code='''pdf = students.filter(F.col("city") == "Pune").toPandas()
print(type(pdf))
print(pdf)

back = spark.createDataFrame(pdf)
print(back.count())''',
         expl='''`toPandas()` is an action that collects the rows to the driver as a pandas DataFrame — handy for plotting a small, already-aggregated result. `createDataFrame(pdf)` goes the other way.

Rule of thumb: aggregate in Spark, then `toPandas()` the small answer. Never `toPandas()` the raw big table.'''),
]

POINTS = '''
1. **SparkSession** is the entry point. `SparkSession.builder.getOrCreate()` returns the running session or starts one.
2. **Driver vs executors**: your Python program is the driver; it plans the job. Executors run tasks on partitions of the data.
3. **Transformations are lazy** (filter, select, withColumn, groupBy, join). **Actions run the job** (show, count, collect, take, first, write, toPandas).

    | Transformation (returns a DataFrame) | Action (returns a result) |
    |---|---|
    | `filter`, `where`, `select`, `withColumn` | `show`, `count` |
    | `groupBy().agg()`, `join`, `orderBy` | `collect`, `take`, `first` |
    | `distinct`, `repartition`, `union` | `toPandas`, `write…save` |

4. **Narrow** transformations keep data in place; **wide** ones (groupBy, join, orderBy, distinct) cause a **shuffle** — an `Exchange` in the plan and a new stage.
5. One **partition** = one **task**. Partitions decide parallelism; uneven partitions (skew) make one task slow.
6. `collect()` brings everything to the driver — use `show()`, `take(n)` or write to files for big data.
7. Read plans with `explain()` bottom-up. Count `Exchange` lines to count shuffles.
8. Use DataFrames, not RDDs, in PySpark: the optimiser can see DataFrame expressions but not Python lambdas.
'''

EXTRA_TITLE = 'Deep dive: how Spark runs your code'
EXTRA_LEAD = 'Chapter 0 described the architecture; these slides connect it to what you just ran.'
EXTRA = [
    dict(t='Job → stages → tasks',
         md='''| Level | Created by | Example |
|---|---|---|
| **Job** | Every action | `count()` = 1 job |
| **Stage** | Splitting the job at each shuffle (`Exchange`) | `groupBy().count()` = 2 stages |
| **Task** | One per partition in a stage | 4 partitions = 4 tasks |''',
         expl='''Inside a stage, Spark pipelines all narrow steps over a partition without writing anything in between. At a stage boundary the map side writes shuffle files and the next stage reads them over the network.

Open the Spark UI at <code>http://localhost:4040</code> while your session runs to see every job, stage and task.'''),
    dict(t='Lineage: how Spark survives a lost executor',
         code='''nums = spark.sparkContext.parallelize(range(10), 2)
plan = nums.map(lambda x: x * 2).filter(lambda x: x > 5)
print(plan.toDebugString().decode())''',
         expl='''Spark does not copy data for safety. It remembers the **lineage** — the chain of steps that produced each partition. If an executor dies, Spark re-runs those steps for just the lost partitions.

The `map` and `filter` lambdas were fused into one `PythonRDD` step, built on the `ParallelCollectionRDD` that holds the original numbers; `(2)` is the partition count. That is the "Resilient" in RDD, and it is why transformations must be deterministic.'''),
]

MCQS = [
    dict(q='Which call starts executing a Spark job?', kind='concept',
         o=['`df.filter(F.col("marks") > 80)`', '`df.select("name")`', '`df.count()`', '`df.withColumn("x", F.lit(1))`'], a=2,
         why='`count()` is an action. The other three are transformations: they only add steps to the plan and return a new DataFrame.'),
    dict(q='What does `local[4]` in `.master("local[4]")` mean?', kind='concept',
         o=['Run on a cluster with 4 machines', 'Run on this machine with 4 worker threads', 'Keep 4 copies of every partition', 'Use 4 GB of memory'], a=1,
         why='`local[n]` runs Spark inside one JVM on the current machine with n worker threads; `local[*]` uses all cores.'),
    dict(q='What does this print?', kind='output', run=True, check='3',
         code='''df = spark.range(10).repartition(3)
print(df.rdd.getNumPartitions())''',
         o=['10', '1', '3', '4'], a=2,
         why='`repartition(3)` shuffles the rows into exactly 3 partitions, whatever the number before.'),
    dict(q='In an `explain()` output, what does an `Exchange` operator mean?', kind='concept',
         o=['A column was renamed', 'Rows are shuffled across partitions — a stage boundary', 'Data is cached in memory', 'A Python UDF is called'], a=1,
         why='`Exchange` is a shuffle. Spark ends a stage there, writes shuffle files and starts a new stage that reads them.'),
    dict(q='Which of these is a **wide** transformation?', kind='concept',
         o=['`filter`', '`withColumn`', '`select`', '`groupBy(...).agg(...)`'], a=3,
         why='Grouping needs every row with the same key on one partition, so it shuffles. filter, withColumn and select work on each partition independently.'),
    dict(q='What does this print?', kind='output', run=True, check="DataFrame[id: bigint]",
         code='''df = spark.range(5).filter("id > 2")
print(df)''',
         o=['`[3, 4]`', '`DataFrame[id: bigint]`', '`2`', 'Nothing — it raises an error'], a=1,
         why='Printing a DataFrame shows only its schema. `filter` is lazy, so no rows are computed until an action such as `show()` or `collect()`.'),
    dict(q='Why is `big_df.collect()` dangerous on a 500 GB table?', kind='concept',
         o=['It deletes the table', 'It brings every row to the driver, which runs out of memory', 'It triggers a shuffle on every column', 'It is slower than `show()` but otherwise safe'], a=1,
         why='`collect()` returns all rows to the single driver process. Use `show()`, `take(n)`, an aggregation or a write instead.'),
    dict(q='The code below fails. Why?', kind='debug', run=True, error=True,
         code='''df = spark.createDataFrame([(1, "a")], ["id", "x"])
df.show().count()''',
         o=['`show()` must come after `count()`', '`show()` prints and returns `None`, so `.count()` is called on `None`', 'DataFrames have no `count()` method', 'A DataFrame needs at least 2 rows'], a=1,
         why='`show()` is an action that prints and returns `None`. Call actions on the DataFrame separately: `df.show()` then `df.count()`.'),
    dict(q='How many **stages** does `students.groupBy("branch").count().show()` need?', kind='concept',
         o=['1', '2', '3', 'One per row'], a=1,
         why='One shuffle (the Exchange for groupBy) splits the job into 2 stages: partial aggregation before the shuffle, final aggregation after it.'),
    dict(q='Why should PySpark code prefer DataFrames over RDDs?', kind='concept',
         o=['RDDs cannot hold strings', 'The optimiser can see DataFrame expressions but not Python lambdas, and data stays in the JVM', 'RDDs are only available in Scala', 'DataFrames never shuffle'], a=1,
         why='Catalyst optimises DataFrame plans and Tungsten runs them in the JVM. RDD lambdas are opaque Python functions, so every row is serialised to Python and back.'),
]

PRACTICE = dict(
    kinds=['Start a session, build a DataFrame from a list, and print its schema.',
           'For each step you write, say out loud: transformation or action? Then check with `explain()`.',
           'Repartition `spark.range(1_000_000)` into 8 partitions and count rows per partition with `spark_partition_id()`.',
           'For the LeetCode problems: build the input table with `spark.createDataFrame`, then solve it with `filter` and `select` only.'],
    note='LeetCode has no PySpark judge. Recreate each problem\'s example table as a DataFrame, write the PySpark answer, and compare your output with the expected output on the problem page.',
    problems=[
        (595, 'big-countries', 'Big Countries', 'Easy', '`filter` with `|` (OR) on two numeric columns.'),
        (1757, 'recyclable-and-low-fat-products', 'Recyclable and Low Fat Products', 'Easy', 'Two equality conditions joined with `&`.'),
        (584, 'find-customer-referee', 'Find Customer Referee', 'Easy', 'NULL trap: `!= 2` drops NULL rows — add `isNull()`.'),
    ])
