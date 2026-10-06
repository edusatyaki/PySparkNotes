ORDER = 9
KEY = '9'
TITLE = 'Reading and Writing Data: CSV, JSON, Parquet, partitions'
WEEK = 'Week 4'
DATA = ['data/raw/orders.csv', 'data/raw/events.jsonl']

SETUP_TITLE = '9.0 The raw files on disk'
SETUP = '''import os, re
from pyspark.sql import functions as F

os.makedirs("data/raw", exist_ok=True)
with open("data/raw/orders.csv", "w") as f:
    f.write("""order_id,order_date,city,amount
1,2026-01-05,Pune,1500.50
2,2026-01-07,Mumbai,1200
3,2026-01-12,Delhi,400
4,2026-01-20,Pune,18000
5,2026-02-02,"Navi Mumbai",900
6,2026-02-03,Delhi,abc
7,2026-02-14,Pune
""")

with open("data/raw/events.jsonl", "w") as f:
    f.write('{"id": 1, "user": "u1", "action": "click"}\\n'
            '{"id": 2, "user": "u2", "action": "buy", "amount": 499}\\n')

def ls(path):
    """List a folder like `ls -R`, hiding .crc files and shortening part-file IDs."""
    for root, dirs, files in sorted(os.walk(path)):
        for name in sorted(files):
            if not name.endswith(".crc"):
                short = re.sub(r"part-(\\d+)-[0-9a-f-]+", r"part-\\1-…", name)
                print(os.path.join(root, short))'''
SETUP_EXPL = '''The CSV has a header, a quoted value with a space (`"Navi Mumbai"`), one **malformed** amount (`abc`) and one **short** row with a missing column — the problems every real file has.

`ls()` is a small helper used on later slides to show which files Spark writes. Spark's part-file names contain a long random ID, which the helper shortens to `…`.'''

SECTIONS = [
    dict(t='9.1 Read a CSV with no options', home=True,
         code='''raw = spark.read.csv("data/raw/orders.csv")
raw.printSchema()
raw.show(3)''',
         expl='''With no options Spark reads the header line as **data**, names the columns `_c0`, `_c1`, … and makes every column a **string**. CSV carries no types, so Spark does not guess unless asked.'''),

    dict(t='9.2 Use the header and let Spark infer types',
         code='''inferred = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/orders.csv"))
inferred.printSchema()
inferred.show()''',
         expl='''`header` takes the column names from the first line. `inferSchema` scans the data to guess types: `order_id` became an integer and `order_date` a date — but `amount` stayed a **string** because of `abc`.

Inference also costs a full extra pass over the files. Fine for exploring; avoid it in pipelines.'''),

    dict(t='9.3 Give the schema yourself',
         code='''schema = "order_id INT, order_date DATE, city STRING, amount DOUBLE"

orders = (spark.read
    .option("header", True)
    .schema(schema)
    .csv("data/raw/orders.csv"))
orders.show()''',
         expl='''With an explicit schema there is no inference pass and the types are what you decided. In the default **PERMISSIVE** mode the value Spark cannot parse becomes NULL (`abc` → NULL amount) and the missing field in row 7 becomes NULL too.

Silently NULL may be acceptable — or may hide a broken feed. The next slides catch the bad rows instead.'''),

    dict(t='9.4 Keep the broken lines for inspection (_corrupt_record)',
         code='''checked = (spark.read
    .option("header", True)
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(schema + ", _corrupt_record STRING")
    .csv("data/raw/orders.csv")
    .cache())

checked.filter(F.col("_corrupt_record").isNotNull()).show(truncate=False)''',
         expl='''Add a string column named `_corrupt_record` to the schema and Spark fills it with the **raw text** of every line it could not parse. Send those rows to a quarantine table and the clean rows onward.

The `cache()` is required: Spark refuses to query only the corrupt-record column straight from the file, because the result would depend on which columns are read.'''),

    dict(t='9.5 Skip bad lines, or fail on them: DROPMALFORMED and FAILFAST',
         code='''dropped = (spark.read.option("header", True).option("mode", "DROPMALFORMED")
                     .schema(schema).csv("data/raw/orders.csv"))
print(dropped.count(), [r.order_id for r in dropped.collect()])

strict = (spark.read.option("header", True).option("mode", "FAILFAST")
                    .schema(schema).csv("data/raw/orders.csv"))
strict.show()''',
         error=True,
         expl='''**`DROPMALFORMED`** silently removes lines that do not fit the schema — rows 6 and 7 are gone. **`FAILFAST`** raises an error at the first bad line, which is what you want when a broken feed must stop the pipeline.

Note that `count()` alone may still report 7, because Spark can count lines without parsing them; collecting the rows forces the parse.''',
         tip='Pick the mode on purpose: PERMISSIVE + _corrupt_record for "keep going and quarantine", FAILFAST for "this file must be perfect".'),

    dict(t='9.6 Write Parquet and read it back',
         code='''clean = orders.dropna(subset=["amount"])
clean.write.mode("overwrite").parquet("data/clean/orders")

ls("data/clean/orders")
back = spark.read.parquet("data/clean/orders")
back.printSchema()
print(back.count())''',
         expl='''A DataFrame is written as a **folder**, not a single file: one `part-…` file per partition plus an empty `_SUCCESS` marker that says the job finished.

Parquet stores the **schema** inside the files, so reading it back needs no options and the types are exact. It is columnar and compressed (snappy by default) — the standard format for analytics.'''),

    dict(t='9.7 Save modes: errorifexists, overwrite, append', error=True,
         code='''clean.write.mode("append").parquet("data/clean/orders")
print(spark.read.parquet("data/clean/orders").count())

clean.write.mode("overwrite").parquet("data/clean/orders")
print(spark.read.parquet("data/clean/orders").count())

clean.write.parquet("data/clean/orders")       # default mode''',
         expl='''`append` adds new files next to the old ones (5 → 10 rows). `overwrite` replaces the whole folder (back to 5). The default mode **`errorifexists`** refuses to write into an existing path; `ignore` would silently skip the write.

Overwrite deletes the old data before the new data is safely written — if the job fails half-way, the old data is gone. Table formats such as Delta Lake make it atomic.'''),

    dict(t='9.8 Partition the output by a column (partitionBy)',
         code='''(clean.withColumn("month", F.date_format("order_date", "yyyy-MM"))
      .write.mode("overwrite")
      .partitionBy("month", "city")
      .parquet("data/lake/orders"))

ls("data/lake/orders")''',
         expl='''`partitionBy` creates one sub-folder per value: `month=2026-01/city=Pune/…`. The partition columns are stored in the **folder names**, not inside the files.

Choose low-cardinality columns that queries filter on — date, month, country. Partitioning by a high-cardinality column (customer ID) creates millions of tiny folders.'''),

    dict(t='9.9 Read only the partitions you need (partition pruning)',
         code='''lake = spark.read.parquet("data/lake/orders")
lake.printSchema()

pune_jan = lake.filter((F.col("city") == "Pune") & (F.col("month") == "2026-01"))
pune_jan.show()
pune_jan.explain()''',
         expl='''Spark rebuilds `month` and `city` from the folder names. The plan shows **`PartitionFilters`**: the filter is applied to folder names before any file is opened, so only `month=2026-01/city=Pune` is read — **partition pruning**.

On a table with 3 years of daily partitions, filtering one day reads about 0.1% of the data.'''),

    dict(t='9.10 Control the number of output files: coalesce and repartition',
         code='''big = spark.range(100_000).withColumn("bucket", F.col("id") % 10)

big.repartition(8).write.mode("overwrite").parquet("data/files/eight")
big.coalesce(1).write.mode("overwrite").parquet("data/files/one")

for p in ["data/files/eight", "data/files/one"]:
    print(p, len([f for f in os.listdir(p) if f.startswith("part-")]), "part files")''',
         expl='''The number of output files equals the number of partitions at write time. `repartition(n)` shuffles into exactly n partitions; `coalesce(n)` merges partitions **without** a shuffle (only to fewer partitions).

`coalesce(1)` gives one file but makes one task write everything — fine for a small export, a bottleneck for big data.'''),

    dict(t='9.11 JSON: one object per line, or a whole document',
         code='''events = spark.read.json("data/raw/events.jsonl")
events.printSchema()
events.show()

with open("data/raw/report.json", "w") as f:
    f.write('[\\n  {"id": 1, "tags": ["a", "b"]},\\n  {"id": 2, "tags": []}\\n]')
spark.read.option("multiLine", True).json("data/raw/report.json").show()''',
         expl='''By default Spark expects **JSON Lines**: one complete object per line. It infers the schema from all fields seen, so `amount` exists with NULL for the click event, and the columns come out sorted by name.

A pretty-printed file or a top-level array spans many lines — read it with `multiLine=True` (one file is then parsed by one task).'''),

    dict(t='9.12 Which file did each row come from? (_metadata)',
         code='''(spark.read.parquet("data/files/eight")
      .select("id", "_metadata.file_name", "_metadata.file_size")
      .groupBy("file_name", "file_size").count()
      .orderBy("file_name")
      .show(3, truncate=False))''',
         expl='''Every file-based DataFrame has a hidden **`_metadata`** struct (Spark 3.3+) with `file_path`, `file_name`, `file_size` and `file_modification_time`. Select it explicitly when you need lineage — which file a bad row came from.'''),

    dict(t='9.13 Write CSV for people (header, single file)',
         code='''(clean.coalesce(1)
      .write.mode("overwrite")
      .option("header", True)
      .csv("data/export/orders_csv"))

ls("data/export/orders_csv")
part = [f for f in os.listdir("data/export/orders_csv") if f.endswith(".csv")][0]
print(open(f"data/export/orders_csv/{part}").read())''',
         expl='''CSV is for handing data to people and other tools, not for storage: no types, no compression, no schema. Spark still writes a folder; `coalesce(1)` makes it a single part file you can rename.

Dates and doubles are written as text in Spark's default format.'''),
]

POINTS = '''
1. `spark.read.format(...).option(...).schema(...).load(path)` — or shortcuts `.csv()`, `.json()`, `.parquet()`.
2. CSV: set `header`, and give an explicit `schema` (DDL string) instead of `inferSchema` in pipelines.
3. Bad records:

    | mode | Bad line becomes | Use for |
    |---|---|---|
    | `PERMISSIVE` (default) | NULLs (+ raw text in `_corrupt_record`) | keep going, quarantine bad rows |
    | `DROPMALFORMED` | dropped silently | rarely — data loss is invisible |
    | `FAILFAST` | an error | feeds that must be perfect |

4. Writes produce a **folder** of `part-` files + `_SUCCESS`. One file per partition.
5. Save modes: `errorifexists` (default), `overwrite`, `append`, `ignore`.
6. `partitionBy(cols)` → `col=value/` folders; filters on those columns are pruned (`PartitionFilters` in the plan). Use low-cardinality columns.
7. Parquet stores the schema, is columnar and compressed — use it (or Delta/Iceberg) for storage; CSV/JSON for exchange.
8. `coalesce(n)` reduces files without a shuffle; `repartition(n)` shuffles to exactly n.
9. `_metadata.file_name` tells you which file each row came from.
'''

EXTRA_TITLE = 'Deep dive: why Parquet is faster'
EXTRA_LEAD = 'Column pruning, predicate pushdown, file size — measured here, not claimed.'
EXTRA = [
    dict(t='Same data: CSV vs Parquet on disk',
         code='''sizes = spark.range(200_000).select(
    "id", (F.col("id") % 100).alias("store"), F.lit("2026-03-01").alias("day"),
    (F.rand(seed=7) * 1000).cast("int").alias("amount"))

sizes.write.mode("overwrite").option("header", True).csv("data/cmp/csv")
sizes.write.mode("overwrite").parquet("data/cmp/parquet")

def size_mb(p):
    return sum(os.path.getsize(os.path.join(p, f)) for f in os.listdir(p) if f.startswith("part-")) / 1e6

print(f"CSV:     {size_mb('data/cmp/csv'):.2f} MB")
print(f"Parquet: {size_mb('data/cmp/parquet'):.2f} MB")''',
         expl='''Parquet stores each column together, so repeated values (`store`, the constant `day`) compress extremely well with dictionary and run-length encoding. Typical real tables shrink 5–10× compared with CSV — less to read from disk and the network.'''),
    dict(t='Column pruning and predicate pushdown in the plan',
         code='''pq = spark.read.parquet("data/cmp/parquet")
pq.filter(F.col("amount") > 990).select("id", "amount").explain()''',
         expl='''`ReadSchema: struct<id:bigint,amount:int>` — only 2 of 4 columns are read from disk (**column pruning**). `PushedFilters: [IsNotNull(amount), GreaterThan(amount,990)]` — the filter is handed to the Parquet reader, which skips whole row groups whose min/max statistics cannot match (**predicate pushdown**).

CSV can do neither: every byte of every line must be read and parsed.'''),
    dict(t='Reading from a database (JDBC)', run=False,
         code='''orders = (spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://db-host:5432/shop")
    .option("dbtable", "public.orders")
    .option("user", "reader")
    .option("password", os.environ["PG_PASSWORD"])
    .option("partitionColumn", "order_id")   # split the read into ranges
    .option("lowerBound", 1).option("upperBound", 10_000_000)
    .option("numPartitions", 8)
    .load())''',
         expl='''Not run in these notes (it needs a PostgreSQL server and its JDBC driver jar). Without `partitionColumn` / `numPartitions` Spark reads the whole table through **one** connection in one task; with them it opens 8 connections, each reading a range of `order_id`.

Filters and column selections are pushed down to the database as a `WHERE` clause and column list. Never hard-code passwords — read them from the environment or a secrets manager.'''),
]

MCQS = [
    dict(q='`spark.read.csv("f.csv")` with no options on a file that has a header line. What are the column names?', kind='concept',
         o=['The names from the header', '`_c0`, `_c1`, …', '`col1`, `col2`, …', 'It fails'], a=1,
         why='Without header=True the first line is data and columns are named _c0, _c1, …, all strings.'),
    dict(q='Which read mode makes the job fail on the first malformed record?', kind='concept',
         o=['PERMISSIVE', 'DROPMALFORMED', 'FAILFAST', 'STRICT'], a=2,
         why='FAILFAST raises an exception as soon as a record cannot be parsed.'),
    dict(q='What happens with `df.write.parquet("out")` when `out` already exists?', kind='concept',
         o=['It overwrites the folder', 'It appends', 'It raises an error (default mode errorifexists)', 'It writes to out_1'], a=2,
         why='The default save mode is errorifexists. Choose overwrite or append explicitly.'),
    dict(q='What does this print?', kind='output', run=True, check='3',
         code='''spark.range(30).repartition(3).write.mode("overwrite").parquet("data/q/three")
print(len([f for f in os.listdir("data/q/three") if f.startswith("part-")]))''',
         o=['1', '3', '30', '200'], a=1,
         why='One part file is written per partition: 3 partitions → 3 files.'),
    dict(q='A table is written with `partitionBy("country")`. A query filters `country = \'IN\'`. What does `PartitionFilters` in the plan mean?', kind='concept',
         o=['Spark reads every file and filters rows', 'Spark skips folders whose country is not IN without opening them', 'Spark repartitions the data by country', 'The filter is ignored'], a=1,
         why='Partition pruning uses folder names (country=IN) to decide which files to read.'),
    dict(q='Why is `partitionBy("customer_id")` usually a bad idea on a large table?', kind='concept',
         o=['customer_id is not a string', 'It creates a huge number of tiny folders and files (high cardinality)', 'Partition columns must be dates', 'It disables Parquet compression'], a=1,
         why='Each distinct value becomes a folder. Millions of tiny files slow listing and reading. Partition by low-cardinality columns.'),
    dict(q='The read below reports every amount as NULL. What is the most likely cause?', kind='debug',
         code='''spark.read.option("header", True) \\
     .schema("order_id INT, amount DOUBLE, city STRING") \\
     .csv("orders.csv")      # file columns: order_id,city,amount''',
         o=['CSV cannot hold doubles', 'The schema order does not match the file: CSV columns are matched by position, so city text is parsed as amount', 'header must be False', 'DOUBLE must be FLOAT'], a=1,
         why='A CSV schema is applied by column position. The second file column (city) is parsed as DOUBLE, which fails → NULL in PERMISSIVE mode.'),
    dict(q='Which file format stores the schema and supports column pruning?', kind='concept',
         o=['CSV', 'JSON Lines', 'Parquet', 'Plain text'], a=2,
         why='Parquet is a typed, columnar format with the schema in the file footer.'),
]

PRACTICE = dict(
    kinds=['Download a real public CSV (for example a city open-data portal export) and load it with an explicit schema.',
           'Quarantine bad rows with _corrupt_record and count them per file using _metadata.file_name.',
           'Write the clean data as Parquet partitioned by month and prove partition pruning with explain().',
           'Compare CSV and Parquet size and read time on the same data.'],
    note='These skills are not covered by LeetCode; practise them on real files.')
