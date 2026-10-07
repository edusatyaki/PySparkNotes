ORDER = 14
KEY = '14'
TITLE = 'Production PySpark: structure, testing, data quality, idempotent writes'
WEEK = 'Week 7'
DATA = ['raw_orders']

SETUP_TITLE = '14.0 The data — a daily raw feed'
SETUP = '''import os
from pyspark.sql import DataFrame, functions as F
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

raw_orders = spark.createDataFrame([
    (1, " pune ", "2026-03-01", "1500"), (2, "Delhi", "2026-03-01", "900"),
    (3, "MUMBAI", "2026-03-02", "-50"),  (4, None, "2026-03-02", "700"),
    (5, "Pune", "2026-03-02", "1200"),   (5, "Pune", "2026-03-02", "1200"),
], "order_id INT, city STRING, order_date STRING, amount STRING")'''
SETUP_EXPL = '''A raw daily feed with the usual problems: messy city names, text amounts, a negative amount, a missing city and a duplicated order.

Notebook code that "works once" becomes production code when it is split into **small, pure functions**, covered by **tests**, guarded by **data-quality checks**, and written so that **re-running it is safe**. This chapter does each step on this feed.'''

SECTIONS = [
    dict(t='14.1 Write transformations as functions: DataFrame in, DataFrame out', tables=['raw_orders'], home=True,
         code='''def clean_city(df: DataFrame) -> DataFrame:
    return df.withColumn("city", F.initcap(F.trim("city")))

def cast_types(df: DataFrame) -> DataFrame:
    return (df.withColumn("order_date", F.to_date("order_date"))
              .withColumn("amount", F.col("amount").cast("double")))

def drop_bad_rows(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["order_id"]).filter((F.col("amount") > 0) & F.col("city").isNotNull())

clean = raw_orders.transform(clean_city).transform(cast_types).transform(drop_bad_rows)
clean.orderBy("order_id").show()''',
         expl='''Each step is a plain function with no side effects: it takes a DataFrame and returns a new one. `df.transform(func)` chains them, so the pipeline reads top to bottom like a recipe.

Small pure functions can be **tested one by one**, reused across jobs and re-ordered safely. Reading files and writing results stay at the edges of the job, outside these functions.'''),

    dict(t='14.2 Parameterised steps: functions that return functions', tables=['raw_orders'],
         code='''def with_amount_band(threshold: float):
    def inner(df: DataFrame) -> DataFrame:
        return df.withColumn("band", F.when(F.col("amount") >= threshold, "high").otherwise("normal"))
    return inner

clean.transform(with_amount_band(1000)).select("order_id", "amount", "band").orderBy("order_id").show()''',
         expl='''When a step needs a setting, return the inner function — `transform(with_amount_band(1000))` stays a one-liner. Since Spark 3.3 `transform` also forwards extra arguments: `df.transform(func, 1000)`.

Thresholds, dates and paths should come from configuration or job arguments, never be hard-coded in the middle of the logic.'''),

    dict(t='14.3 Test a transformation: assertDataFrameEqual',
         code='''def test_clean_city():
    given = spark.createDataFrame([(1, "  pune "), (2, "DELHI")], "order_id INT, city STRING")
    expected = spark.createDataFrame([(1, "Pune"), (2, "Delhi")], "order_id INT, city STRING")
    assertDataFrameEqual(clean_city(given), expected)
    print("test_clean_city passed")

test_clean_city()''',
         expl='''`pyspark.testing.assertDataFrameEqual` (Spark 3.5+) compares two DataFrames — schema and rows — and ignores row order by default. Build tiny inputs by hand, including the edge cases you care about.

In a real project this function lives in `tests/test_transforms.py` and runs with **pytest**, using one SparkSession shared by all tests (a session-scoped fixture), because starting Spark takes seconds.'''),

    dict(t='14.4 When a test fails, the message shows the difference', error=True,
         code='''given = spark.createDataFrame([(1, "  pune "), (2, "navi  mumbai")], "order_id INT, city STRING")
expected = spark.createDataFrame([(1, "Pune"), (2, "Navi Mumbai")], "order_id INT, city STRING")
assertDataFrameEqual(clean_city(given), expected)''',
         expl='''The test found a real bug: `trim` does not collapse the double space inside "navi  mumbai" (Chapter 4 had the fix — `regexp_replace("\\s+", " ")`). The error lists the rows that differ, so you can see exactly what came out.

This is why tests use **edge cases**: the happy path passed on the previous slide.'''),

    dict(t='14.5 Check the schema of the output: assertSchemaEqual',
         code='''expected_schema = "order_id INT, city STRING, order_date DATE, amount DOUBLE"
out = raw_orders.transform(clean_city).transform(cast_types)

assertSchemaEqual(out.schema, spark.createDataFrame([], expected_schema).schema)
print("schema OK:", out.schema.simpleString())''',
         expl='''A downstream table, dashboard or model depends on column names and types. A schema test catches the day someone renames a column or a cast silently changes a type.

Build the expected schema from a DDL string — it is readable and easy to review.'''),

    dict(t='14.6 Data-quality checks before writing', tables=['raw_orders'],
         code='''def quality_report(df: DataFrame) -> dict:
    r = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("city").isNull().cast("int")).alias("null_city"),
        F.sum((F.col("amount") <= 0).cast("int")).alias("non_positive_amount"),
        (F.count("*") - F.countDistinct("order_id")).alias("duplicate_ids"),
    ).first().asDict()
    return r

before = quality_report(raw_orders.transform(clean_city).transform(cast_types))
after = quality_report(clean)
print("before:", before)
print("after: ", after)
assert after["null_city"] == 0 and after["duplicate_ids"] == 0, "quality gate failed"''',
         expl='''Compute the checks in **one** aggregation pass and fail the job (`assert` or raise) if a rule is broken — before bad data reaches a table people trust. Log the report every run; trends in these numbers are an early warning of upstream changes.

Libraries such as Great Expectations or Delta Live Tables expectations package the same idea.'''),

    dict(t='14.7 Re-runs must be safe: dynamic partition overwrite',
         code='''spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def write_day(df: DataFrame, path: str):
    df.write.mode("overwrite").partitionBy("order_date").parquet(path)

write_day(clean, "data/prod/orders")                          # first load: both days
fix = clean.filter("order_date = '2026-03-02'").withColumn("amount", F.col("amount") + 1)
write_day(fix, "data/prod/orders")                             # re-run for 2026-03-02 only

spark.read.parquet("data/prod/orders").orderBy("order_id").show()''',
         expl='''With the default (`static`) mode, `overwrite` deletes the **whole** folder — re-running one day would wipe every other day. With `partitionOverwriteMode=dynamic`, Spark replaces **only the partitions present in the new data**: 2026-03-02 was rewritten, 2026-03-01 kept.

That makes a daily job **idempotent**: running it twice for the same day gives the same result as running it once — the property every scheduler retry relies on.'''),

    dict(t='14.8 Log what the job did', tables=['raw_orders'],
         code='''import logging, json
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s", force=True)
logging.getLogger("py4j").setLevel(logging.WARNING)     # keep PySpark's own chatter out
log = logging.getLogger("orders_job")

metrics = {"input_rows": raw_orders.count(), "output_rows": clean.count(),
           "dropped": raw_orders.count() - clean.count(), "run_date": "2026-03-02"}
log.info(json.dumps(metrics))
print(json.dumps(metrics))''',
         expl='''Use Python's `logging` on the driver, one structured (JSON) line per important event: row counts in and out, rows dropped, the run date. Log aggregates, never raw rows — logs are often less protected than tables.

Each `count()` is a job; in large pipelines reuse numbers you already computed (the quality report) instead of counting again.'''),
]

POINTS = '''
1. Write transformations as **pure functions** `DataFrame → DataFrame`; chain them with `df.transform(func)`. Keep I/O at the edges.
2. Configuration (dates, paths, thresholds) comes from arguments or config files, not from the middle of the code.
3. Test with `pyspark.testing.assertDataFrameEqual` / `assertSchemaEqual` and pytest; build tiny hand-made inputs with edge cases; share one SparkSession per test run.
4. Run **data-quality checks** in one aggregation and fail the job before writing bad data.
5. Make jobs **idempotent**: `partitionOverwriteMode=dynamic` + `partitionBy(date)`, or MERGE into a table format.
6. Log structured metrics (rows in/out/dropped) on the driver.

    | Layer | Typical content |
    |---|---|
    | **Bronze** | Raw data as received, append-only |
    | **Silver** | Cleaned, typed, deduplicated |
    | **Gold** | Business aggregates for reports and ML |
'''

EXTRA_TITLE = 'Deep dive: shipping the job'
EXTRA_LEAD = 'How the code is organised and how it runs on a cluster. (Delta Lake, MERGE and the medallion layers are in Chapter 12.)'
EXTRA = [
    dict(t='A project layout that scales',
         md='''| Path | Holds |
|---|---|
| `src/orders_job/transforms.py` | Pure DataFrame → DataFrame functions |
| `src/orders_job/io.py` | Readers and writers (paths, formats, schemas) |
| `src/orders_job/main.py` | Parses arguments, builds the SparkSession, wires I/O and transforms |
| `tests/conftest.py` | Session-scoped `spark` fixture for pytest |
| `tests/test_transforms.py` | One test per transform, with edge cases |
| `pyproject.toml` | Dependencies; build a wheel for the cluster |''',
         expl='''Only `main.py` knows about the outside world. Everything in `transforms.py` can be tested on a laptop in seconds.'''),
    dict(t='Submitting the job to a cluster', run=False, file='terminal',
         code='''spark-submit \\
  --master yarn --deploy-mode cluster \\
  --num-executors 20 --executor-cores 5 --executor-memory 16g \\
  --conf spark.sql.shuffle.partitions=400 \\
  --conf spark.sql.sources.partitionOverwriteMode=dynamic \\
  --py-files dist/orders_job-1.0-py3-none-any.whl \\
  src/orders_job/main.py --run-date 2026-03-02''',
         expl='''Not run in these notes (it needs a cluster). `spark-submit` starts the driver and executors, ships your code (`--py-files` takes a wheel or zip), and passes arguments to `main.py`. In `cluster` deploy mode the driver runs on the cluster, so the job survives your laptop disconnecting.

Schedulers (Airflow, Databricks Workflows, cron) call exactly this command once per day with a different `--run-date`.'''),
]

MCQS = [
    dict(q='What does `df.transform(clean_city)` do?', kind='concept',
         o=['Runs clean_city on every row in Python', 'Calls clean_city(df) and returns its result, so functions can be chained', 'Caches df', 'Registers a UDF'], a=1,
         why='transform simply applies a DataFrame → DataFrame function, which makes pipelines read as a chain.'),
    dict(q='A daily job writes with `mode("overwrite").partitionBy("date")` and the default settings. You re-run it for one date. What happens to the other dates?', kind='concept',
         o=['They are kept', 'They are deleted — static overwrite replaces the whole folder', 'They are duplicated', 'The write fails'], a=1,
         why='Static mode deletes the full output path. Set partitionOverwriteMode=dynamic to replace only the partitions being written.'),
    dict(q='Which property means "running the job twice gives the same result as running it once"?', kind='concept',
         o=['Atomicity', 'Idempotency', 'Immutability', 'Lazy evaluation'], a=1,
         why='Idempotent jobs are safe to retry, which schedulers do automatically after failures.'),
    dict(q='What does this print?', kind='output', run=True, check='equal',
         code='''a = spark.createDataFrame([(1, "x"), (2, "y")], "id INT, v STRING")
b = spark.createDataFrame([(2, "y"), (1, "x")], "id INT, v STRING")
assertDataFrameEqual(a, b)
print("equal")''',
         o=['`equal`', 'AssertionError: rows are in a different order', 'AssertionError: schemas differ', 'Nothing'], a=0,
         why='assertDataFrameEqual ignores row order by default (checkRowOrder=False).'),
    dict(q='Why should tests share one SparkSession (a session-scoped pytest fixture)?', kind='concept',
         o=['Spark allows only one test', 'Starting a SparkSession takes seconds; one per test makes the suite slow', 'Each session uses a new Java version', 'Sessions cannot be stopped'], a=1,
         why='Session start-up dominates test time; reuse it across the whole test run.'),
    dict(q='Which capability does Delta Lake add on top of plain Parquet folders?', kind='concept',
         o=['Columnar storage', 'Compression', 'ACID transactions and MERGE / time travel', 'Partition pruning'], a=2,
         why='Parquet is already columnar and compressed and supports pruning; the transaction log adds ACID writes, MERGE and time travel.'),
]

PRACTICE = dict(
    kinds=['Turn one of your notebooks into transforms.py + main.py + tests with pytest.',
           'Add a quality gate that fails the job when duplicates or NULL keys appear.',
           'Write a daily job with dynamic partition overwrite; run it twice for the same day and prove nothing changed.',
           'Install delta-spark in a fresh environment that matches its supported Spark version and try MERGE and time travel.'],
    note='Production habits are best practised on your own capstone project.')
