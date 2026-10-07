ORDER = 12
KEY = '12'
TITLE = 'Delta Lake and the Medallion Architecture: MERGE, SCD2, incremental loads'
DELTA = True     # run on a Spark session with Delta Lake
WEEK = 'Week 6'
DATA = ['customers (Delta)', 'dim_customer (SCD2)', 'orders_source']

SETUP_TITLE = '12.0 A Spark session with Delta Lake'
SETUP = '''# pip install delta-spark==4.4.1      (matches Spark 4.2)
import os, re, shutil
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

builder = (SparkSession.builder.appName("delta-notes")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()

shutil.rmtree("data/delta", ignore_errors=True)
customers = spark.createDataFrame([
    (1, "Amit", "Pune", "2026-01-10"), (2, "Riya", "Mumbai", "2026-01-12"),
    (3, "Arjun", "Delhi", "2026-01-15"),
], "customer_id INT, name STRING, city STRING, updated_at STRING") \\
    .withColumn("updated_at", F.to_date("updated_at"))'''
SETUP_EXPL = '''**Delta Lake** is an open table format: ordinary Parquet files plus a **transaction log** (`_delta_log/`) that records every change. The two `config` lines register Delta's SQL commands and catalog; `configure_spark_with_delta_pip` adds the matching Delta JARs to the session.

Databricks has Delta built in; on plain Spark you add the `delta-spark` package whose version matches your Spark version.'''

SECTIONS = [
    dict(divider=True, t='The medallion architecture in full',
         lead='Chapters 3, 4 and 5 each built one layer with plain Parquet. Here are the rules for all three, and why Delta is the format teams use for them.'),

    dict(t='12.1 Bronze, Silver, Gold — the rules of each layer',
         md='''| | **Bronze** | **Silver** | **Gold** |
|---|---|---|---|
| Purpose | Keep the data exactly as it arrived | Clean, conformed, trustworthy records | Answers to business questions |
| Typical content | Raw rows as strings + `_source_file`, `_ingest_date` | Typed columns, deduplicated, validated, one row per key | Aggregates, KPIs, wide tables for BI and ML features |
| Write pattern | Append only | MERGE (upsert) by key, SCD2 for history | Overwrite or MERGE per period |
| Data quality | None — never reject | Rules enforced, bad rows quarantined | Business rules, reconciled totals |
| Who reads it | Data engineers (replay, debugging) | Engineers, analysts, data scientists | Dashboards, reports, models, APIs |
| Built in | Chapter 3 | Chapter 4 | Chapter 5 |''',
         expl='''Data flows one way: Bronze → Silver → Gold. Because Bronze keeps everything, a bug in a Silver rule is fixed by changing the rule and **re-running from Bronze** — no source system has to re-send anything.'''),

    dict(t='12.2 Why plain Parquet folders are not enough',
         md='''| Problem with a Parquet folder | What Delta's transaction log adds |
|---|---|
| A job dies half-way through an overwrite: the old data is gone, the new data is incomplete | **ACID transactions**: a write commits fully or not at all |
| Correcting one customer means rewriting files by hand | `MERGE`, `UPDATE`, `DELETE` |
| A bad load cannot be undone | **Time travel** and `RESTORE` to an earlier version |
| Readers see files appear one by one during a write | Readers always see the last committed version |
| A new column or wrong type silently enters the table | **Schema enforcement**, and evolution only when you ask |
| Small files pile up after many appends | `OPTIMIZE` compaction, `VACUUM` of old files |''',
         expl='''Apache Iceberg and Apache Hudi solve the same problems with a similar design; the ideas in this chapter carry over. These notes use Delta because it ships with Databricks and has a pip package for local Spark.'''),

    dict(divider=True, t='Delta Lake basics'),

    dict(t='12.3 Write a Delta table and look inside the folder', tables=['customers'],
         code='''customers.write.format("delta").save("data/delta/customers")

for root, dirs, files in sorted(os.walk("data/delta/customers")):
    for f in sorted(files):
        if not f.endswith(".crc"):
            print(re.sub(r"part-(\\d+)-[0-9a-f-]+", r"part-\\1-…", os.path.join(root, f)))''',
         expl='''`format("delta")` writes normal Parquet data files (one per partition of the DataFrame; the long file IDs are shortened to `…` here) plus `_delta_log/00000000000000000000.json` — **version 0** of the table. Every later write adds a new numbered JSON file listing which data files were added and removed.

A reader first reads the log to find out which Parquet files make up the current version; files not in the log are invisible.'''),

    dict(t='12.4 Every write is a version: history and time travel', tables=['customers'],
         code='''more = spark.createDataFrame([(4, "Sneha", "Pune", "2026-02-01")],
                             "customer_id INT, name STRING, city STRING, updated_at STRING") \\
             .withColumn("updated_at", F.to_date("updated_at"))
more.write.format("delta").mode("append").save("data/delta/customers")

dt = DeltaTable.forPath(spark, "data/delta/customers")
dt.history().select("version", "operation", "operationMetrics.numOutputRows").show()

print("now:", spark.read.format("delta").load("data/delta/customers").count(), "rows")
print("v0: ", spark.read.format("delta").option("versionAsOf", 0).load("data/delta/customers").count(), "rows")''',
         expl='''`DeltaTable.history()` (SQL: `DESCRIBE HISTORY`) lists every commit: version, operation, and metrics such as rows written. `option("versionAsOf", n)` — or `timestampAsOf` — reads the table **as it was** at that version.

Time travel is how you audit a change, reproduce last week's report exactly, or compare before and after a load.'''),

    dict(t='12.5 Schema enforcement: unexpected columns are refused', error=True,
         code='''with_email = spark.createDataFrame([(5, "Karan", "Delhi", None, "karan@mail.com")],
    "customer_id INT, name STRING, city STRING, updated_at DATE, email STRING")

with_email.write.format("delta").mode("append").save("data/delta/customers")''',
         expl='''Plain Parquet would happily write files with an extra column and leave the folder inconsistent. Delta checks the incoming schema against the table's and **refuses** the write: `DELTA_METADATA_MISMATCH`.

That protects every Silver and Gold table from an upstream change nobody announced.'''),

    dict(t='12.6 Schema evolution, when you decide to allow it',
         code='''(with_email.write.format("delta").mode("append")
           .option("mergeSchema", "true")
           .save("data/delta/customers"))

spark.read.format("delta").load("data/delta/customers").orderBy("customer_id").show()''',
         expl='''`mergeSchema=true` adds the new column to the table schema in the same commit. Existing rows read it as NULL. Evolution is a deliberate, recorded decision — visible in the history — not an accident.'''),

    dict(t='12.7 UPDATE and DELETE on a lake table',
         code='''dt = DeltaTable.forPath(spark, "data/delta/customers")
dt.update(condition="customer_id = 2", set={"city": "'Pune'"})
dt.delete("customer_id = 5")

spark.read.format("delta").load("data/delta/customers").orderBy("customer_id").show()
dt.history().select("version", "operation").show()''',
         expl='''`update(condition, set)` and `delete(condition)` change individual rows. Behind the scenes Delta rewrites only the data files that contain matching rows and commits "remove old file, add new file" to the log. The values in `set` are SQL expressions, so a string needs inner quotes: `"'Pune'"`.'''),

    dict(divider=True, t='MERGE: the upsert behind every Silver table'),

    dict(t='12.8 Upsert a batch of changes (MERGE)',
         code='''changes = spark.createDataFrame([
    (1, "Amit", "Bengaluru", "2026-03-01"),    # existing customer, new city
    (6, "Meera", "Chennai",  "2026-03-01"),    # new customer
], "customer_id INT, name STRING, city STRING, updated_at STRING") \\
    .withColumn("updated_at", F.to_date("updated_at"))

(dt.alias("t")
   .merge(changes.alias("s"), "t.customer_id = s.customer_id")
   .whenMatchedUpdate(set={"city": "s.city", "updated_at": "s.updated_at"})
   .whenNotMatchedInsert(values={"customer_id": "s.customer_id", "name": "s.name",
                                 "city": "s.city", "updated_at": "s.updated_at"})
   .execute())

spark.read.format("delta").load("data/delta/customers").orderBy("customer_id").show()''',
         expl='''`MERGE` compares source rows with target rows on a key: **matched** rows are updated, **not matched** rows are inserted — one atomic commit. Amit moved to Bengaluru; Meera is new.

`whenMatchedUpdateAll()` / `whenNotMatchedInsertAll()` copy every column when the names match; `whenNotMatchedBySourceDelete()` also removes target rows missing from the source.'''),

    dict(t='12.9 The same MERGE in SQL',
         code='''spark.sql(f"CREATE TABLE IF NOT EXISTS customers USING DELTA "
          f"LOCATION '{os.path.abspath('data/delta/customers')}'")

spark.sql("""
    MERGE INTO customers AS t
    USING (SELECT 3 AS customer_id, 'Arjun' AS name, 'Noida' AS city,
                  DATE'2026-03-05' AS updated_at) AS s
    ON t.customer_id = s.customer_id
    WHEN MATCHED AND s.updated_at > t.updated_at
        THEN UPDATE SET t.city = s.city, t.updated_at = s.updated_at
    WHEN NOT MATCHED
        THEN INSERT (customer_id, name, city, updated_at)
             VALUES (s.customer_id, s.name, s.city, s.updated_at)
""").show()

spark.table("customers").filter("customer_id = 3").show()''',
         expl='''`CREATE TABLE … USING DELTA LOCATION` registers the existing folder under a name in the catalog — no data is copied — so SQL can say `MERGE INTO customers`. The extra condition `s.updated_at > t.updated_at` makes the merge ignore **stale** updates that arrive late — a common guard in real pipelines.

The columns are listed explicitly: `UPDATE SET *` / `INSERT *` would require the source to have **every** target column, including the `email` added on slide 12.6. The statement returns the merge metrics: rows affected, updated, deleted and inserted.'''),

    dict(t='12.10 SCD Type 1 vs Type 2',
         md='''| | **SCD Type 1** | **SCD Type 2** |
|---|---|---|
| On change | Overwrite the old value | Close the old row, add a new row |
| History | Lost | Kept, with validity dates |
| Columns added | None | `start_date`, `end_date`, `is_current` |
| Question it can answer | "Where does Amit live now?" | "Where did Amit live when he placed the January order?" |
| Implemented by | The MERGE on slide 12.8 | The MERGE on the next slide |''',
         expl='''SCD = **slowly changing dimension**: attributes such as a customer's city that change rarely, but whose history matters for correct reporting. Revenue by city for January must use the city the customer had **in January**.'''),

    dict(t='12.11 SCD Type 2 with one MERGE',
         code='''dim_customer = (spark.read.format("delta").load("data/delta/customers")
    .filter("customer_id <= 3")
    .select("customer_id", "name", "city",
            F.to_date(F.lit("2026-01-01")).alias("start_date"),
            F.lit(None).cast("date").alias("end_date"),
            F.lit(True).alias("is_current")))
dim_customer.write.format("delta").save("data/delta/dim_customer")
dim = DeltaTable.forPath(spark, "data/delta/dim_customer")

updates = spark.createDataFrame([(1, "Amit", "Hyderabad", "2026-04-01"), (2, "Riya", "Pune", "2026-04-01"),
                                 (7, "Dev", "Jaipur", "2026-04-01")],
                                "customer_id INT, name STRING, city STRING, eff STRING") \\
    .withColumn("eff", F.to_date("eff"))

current = dim.toDF().filter("is_current")
changed = updates.alias("u").join(current.alias("c"), "customer_id").filter("u.city <> c.city").select("u.*")
staged = (changed.withColumn("merge_key", F.lit(None).cast("int"))          # rows to INSERT as new versions
          .unionByName(updates.withColumn("merge_key", F.col("customer_id"))))  # rows to match and close

(dim.alias("t").merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current")
    .whenMatchedUpdate(condition="t.city <> s.city", set={"is_current": "false", "end_date": "s.eff"})
    .whenNotMatchedInsert(values={"customer_id": "s.customer_id", "name": "s.name", "city": "s.city",
                                  "start_date": "s.eff", "end_date": "null", "is_current": "true"})
    .execute())

dim.toDF().orderBy("customer_id", "start_date").show()''',
         expl='''The trick is the **staged** source. Every update appears once with `merge_key = customer_id`: it matches the customer's current row, and if the city changed, that row is **closed** (`is_current = false`, `end_date` set). Changed customers appear a second time with `merge_key = NULL`, which never matches, so they are **inserted** as the new current version.

Result: Amit has a closed Bengaluru row (his city after slide 12.8) and a current Hyderabad row; Riya (already Pune) is untouched; Dev is new. Arjun was not in the batch and keeps his row.'''),

    dict(divider=True, t='Incremental loads'),

    dict(t='12.12 Load only what changed since last time (a high-water mark)',
         code='''source = spark.createDataFrame([
    (101, 1, 1500.0, "2026-03-01 09:00:00"), (102, 2,  900.0, "2026-03-01 10:00:00"),
    (103, 1,  300.0, "2026-03-02 11:00:00"),
], "order_id INT, customer_id INT, amount DOUBLE, updated_at STRING") \\
    .withColumn("updated_at", F.to_timestamp("updated_at"))

def incremental_load(src):
    path = "data/delta/silver_orders"
    if not DeltaTable.isDeltaTable(spark, path):
        src.write.format("delta").save(path)                       # first run: full load
        return src.count()
    hwm = spark.read.format("delta").load(path).agg(F.max("updated_at")).first()[0]
    new = src.filter(F.col("updated_at") > F.lit(hwm))            # only rows after the high-water mark
    (DeltaTable.forPath(spark, path).alias("t")
        .merge(new.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    return new.count()

print("run 1 loaded", incremental_load(source))
source2 = source.unionByName(spark.createDataFrame(
    [(102, 2, 950.0, "2026-03-03 08:00:00"), (104, 3, 700.0, "2026-03-03 09:00:00")],
    "order_id INT, customer_id INT, amount DOUBLE, updated_at STRING")
    .withColumn("updated_at", F.to_timestamp("updated_at")))
print("run 2 loaded", incremental_load(source2))
print("run 3 loaded", incremental_load(source2))
spark.read.format("delta").load("data/delta/silver_orders").orderBy("order_id").show()''',
         expl='''A **high-water mark** is the largest `updated_at` already loaded. Each run reads only source rows newer than it and **MERGEs** them, so a changed order (102, corrected to 950) is updated in place and a new order (104) is inserted. Run 3 finds nothing new and changes nothing — the load is **idempotent**.

For files instead of a database, Structured Streaming with `availableNow` (Chapter 13) gives the same "only new data" behaviour through checkpoints.'''),

    dict(t='12.13 Pass changes downstream: the Change Data Feed',
         code='''spark.sql(f"CREATE TABLE IF NOT EXISTS silver_orders USING DELTA "
          f"LOCATION '{os.path.abspath('data/delta/silver_orders')}'")
spark.sql("ALTER TABLE silver_orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
start = DeltaTable.forPath(spark, "data/delta/silver_orders").history(1).first()["version"] + 1

DeltaTable.forPath(spark, "data/delta/silver_orders").update(
    condition="order_id = 101", set={"amount": "1600.0"})

(spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", start)
      .load("data/delta/silver_orders")
      .select("order_id", "amount", "_change_type", "_commit_version").show())''',
         expl='''With the **Change Data Feed** on, Delta records row-level changes. Reading with `readChangeFeed` returns each changed row with `_change_type`: `insert`, `delete`, or the `update_preimage` / `update_postimage` pair (before and after).

Gold jobs use it to update only the aggregates affected by changed Silver rows instead of recomputing everything.'''),

    dict(divider=True, t='Undo and maintenance'),

    dict(t='12.14 Undo a bad load: RESTORE',
         code='''orders_dt = DeltaTable.forPath(spark, "data/delta/silver_orders")
good_version = orders_dt.history(1).first()["version"]

orders_dt.delete("amount > 0")                              # oops: deletes everything
print("after the mistake:", spark.read.format("delta").load("data/delta/silver_orders").count(), "rows")

orders_dt.restoreToVersion(good_version)
print("after restore:   ", spark.read.format("delta").load("data/delta/silver_orders").count(), "rows")
orders_dt.history(3).select("version", "operation").show()''',
         expl='''`restoreToVersion(n)` (SQL: `RESTORE TABLE … TO VERSION AS OF n`) makes an old version current again. It is itself a new commit, so even the restore can be undone.

The old data files are still there because Delta only marks them as removed in the log; they are physically deleted later by `VACUUM`.'''),

    dict(t='12.15 Compact small files: OPTIMIZE, then VACUUM',
         code='''for i in range(5):                                         # five tiny appends → many small files
    spark.range(i * 10, i * 10 + 10).write.format("delta").mode("append").save("data/delta/events")

ev = DeltaTable.forPath(spark, "data/delta/events")
print("files before:", ev.detail().first()["numFiles"])
m = ev.optimize().executeCompaction().first()["metrics"]
print("removed", m["numFilesRemoved"], "added", m["numFilesAdded"])
print("files after: ", ev.detail().first()["numFiles"])''',
         expl='''Frequent small appends (streaming, hourly loads) create many small files, and reading many small files is slow. `OPTIMIZE` rewrites them into fewer large ones in one commit; readers are not disturbed.

`VACUUM` then deletes data files no longer referenced by any version newer than the retention period (7 days by default). After vacuuming, time travel beyond that period is no longer possible — that is the trade-off.'''),
]

POINTS = '''
1. **Medallion**: Bronze (raw, append-only, lineage) → Silver (clean, typed, MERGE by key) → Gold (business aggregates). Rebuild downstream from Bronze when rules change.
2. **Delta Lake** = Parquet files + a `_delta_log` transaction log → ACID writes, time travel, MERGE/UPDATE/DELETE, schema enforcement.
3. Write: `df.write.format("delta").save(path)`; read: `spark.read.format("delta").load(path)`; old versions: `option("versionAsOf", n)`.
4. Schema changes are refused unless you pass `mergeSchema=true`.
5. **MERGE** = upsert: `whenMatchedUpdate…`, `whenNotMatchedInsert…`, `whenNotMatchedBySourceDelete`.

<!--break-->

6. **SCD1** overwrites; **SCD2** closes the old row (`end_date`, `is_current=false`) and inserts a new one — one MERGE with a staged source (`merge_key` NULL for the new versions).
7. **Incremental loads**: high-water mark on `updated_at` + MERGE → idempotent re-runs. The **Change Data Feed** passes row-level changes downstream.
8. `RESTORE` undoes a bad load; `OPTIMIZE` compacts small files; `VACUUM` deletes unreferenced files (and limits time travel).

    | Operation | Python | SQL |
    |---|---|---|
    | History | `DeltaTable.forPath(spark, p).history()` | `DESCRIBE HISTORY t` |
    | Read old version | `.option("versionAsOf", 3)` | `SELECT * FROM t VERSION AS OF 3` |
    | Upsert | `dt.alias("t").merge(src.alias("s"), cond)…execute()` | `MERGE INTO t USING s ON …` |
    | Undo | `dt.restoreToVersion(3)` | `RESTORE TABLE t TO VERSION AS OF 3` |
    | Compact | `dt.optimize().executeCompaction()` | `OPTIMIZE t` |
'''

MCQS = [
    dict(q='What makes a Delta table different from a plain folder of Parquet files?', kind='concept',
         o=['Delta uses CSV files', 'A transaction log (_delta_log) that records every commit', 'Delta stores data in memory only', 'Delta does not support partitions'], a=1,
         why='The log turns a folder of Parquet files into a table with ACID commits, versions and row-level operations.'),
    dict(q='In which layer should a cleaning rule (trim names, cast types, drop invalid phones) be applied?', kind='concept',
         o=['Bronze', 'Silver', 'Gold', 'In the dashboard'], a=1,
         why='Bronze keeps raw data untouched; Silver applies cleaning and validation; Gold aggregates.'),
    dict(q='A customer moves from Pune to Delhi. With SCD Type 2, what happens to the dimension table?', kind='concept',
         o=['The city is overwritten', 'The Pune row is closed (end_date, is_current=false) and a new Delhi row is inserted', 'Both rows are deleted', 'Nothing — SCD2 ignores changes'], a=1,
         why='SCD2 keeps history: the old version is closed and a new current version is added.'),
    dict(q='An append with an extra column fails with a schema mismatch. What allows it deliberately?', kind='debug',
         code='''df.write.format("delta").mode("append").save(path)''',
         o=['`.mode("overwrite")`', '`.option("mergeSchema", "true")`', '`.option("inferSchema", "true")`', 'Cast the column to string'], a=1,
         why='mergeSchema adds new columns to the table schema in the same commit.'),
    dict(q='What does this print?', kind='output', run=True, check='2',
         code='''spark.range(3).write.format("delta").mode("overwrite").save("data/delta/q1")
spark.range(5).write.format("delta").mode("overwrite").save("data/delta/q1")
print(DeltaTable.forPath(spark, "data/delta/q1").history().count())''',
         o=['1', '2', '3', '5'], a=1,
         why='Each write is one commit: two overwrites → versions 0 and 1 → two history rows.'),
    dict(q='Why does an incremental load with a high-water mark plus MERGE make re-runs safe?', kind='concept',
         o=['It deletes the target first', 'Rows already loaded are either skipped (older than the mark) or matched and updated, never duplicated', 'It uses append mode', 'It caches the source'], a=1,
         why='Filtering by the mark and upserting by key means running twice gives the same table as running once.'),
    dict(q='Someone ran a DELETE without a WHERE on a Delta table an hour ago. What is the fastest fix?', kind='concept',
         o=['Reload everything from the source systems', '`RESTORE` the table to the version before the delete', 'Run VACUUM', 'Nothing can be done'], a=1,
         why='Old data files are still referenced by earlier versions; RESTORE makes that version current again (until VACUUM removes them).'),
    dict(q='What is the trade-off of running `VACUUM` with a short retention?', kind='concept',
         o=['Queries become slower', 'You can no longer time-travel to versions whose files were deleted', 'The table schema is reset', 'MERGE stops working'], a=1,
         why='VACUUM physically deletes unreferenced files, so older versions can no longer be read.'),
]

PRACTICE = dict(
    kinds=['Convert your Chapter 3–5 Bronze, Silver and Gold Parquet folders into Delta tables.',
           'Write the Silver step as a MERGE by key, run it twice with the same input, and prove nothing changes.',
           'Implement SCD2 for a product dimension whose price changes; query the price on a given date.',
           'Break a table with a bad DELETE, then RESTORE it, and read the history.'],
    note='Delta also runs on Databricks Free Edition, where these commands work without any setup.')
