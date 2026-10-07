ORDER = 13
KEY = '13'
TITLE = 'Structured Streaming: files, windows, watermarks, foreachBatch'
WEEK = 'Week 6'
DATA = ['data/stream/in/*.json']

SETUP_TITLE = '13.0 A folder that receives new files'
SETUP = '''import json, os, shutil
from pyspark.sql import functions as F

for d in ["data/stream"]:
    shutil.rmtree(d, ignore_errors=True)
os.makedirs("data/stream/in")

def drop_file(name, events):
    """Simulate an upstream system writing one new JSON-lines file into the folder."""
    tmp = f"data/stream/{name}.tmp"
    with open(tmp, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\\n")
    os.rename(tmp, f"data/stream/in/{name}.json")   # appear atomically

drop_file("batch1", [
    {"order_id": 1, "ts": "2026-03-01 10:00:05", "city": "Pune",   "amount": 500},
    {"order_id": 2, "ts": "2026-03-01 10:03:40", "city": "Delhi",  "amount": 1200},
    {"order_id": 3, "ts": "2026-03-01 10:07:10", "city": "Pune",   "amount": 300},
    {"order_id": 4, "ts": "2026-03-01 10:12:00", "city": "Mumbai", "amount": 900},
])

schema = "order_id INT, ts TIMESTAMP, city STRING, amount INT"'''
SETUP_EXPL = '''Structured Streaming treats a stream as a **table that keeps growing**: every new record is a new row, and your query is re-run incrementally on the new rows only.

Here the stream source is a folder: each new JSON file is a new batch of orders. `drop_file` writes to a temporary name and renames it, because a file source must never see half-written files. The same code works with Kafka by changing only the `readStream` source.'''

SECTIONS = [
    dict(t='13.1 Read a stream: readStream with a schema',
         code='''orders = spark.readStream.schema(schema).json("data/stream/in")

print(orders.isStreaming)
orders.printSchema()''',
         expl='''`spark.readStream` returns a **streaming DataFrame**. Most of the DataFrame API — `select`, `filter`, `withColumn`, `groupBy`, joins — works on it unchanged.

A file stream **requires** a schema: Spark cannot infer one from data that has not arrived yet. Calling `show()` or `count()` directly on a streaming DataFrame is an error — results are produced by starting a query with `writeStream`.'''),

    dict(t='13.2 Start a query: writeStream, a sink, a trigger',
         code='''big = orders.filter(F.col("amount") >= 500).select("order_id", "city", "amount")

q = (big.writeStream
        .format("memory").queryName("big_orders")     # results go to an in-memory table
        .outputMode("append")
        .trigger(availableNow=True)                   # process what is there, then stop
        .start())
q.awaitTermination()

spark.sql("SELECT * FROM big_orders ORDER BY order_id").show()''',
         expl='''`writeStream` needs a **sink** (`memory` here — for testing only; real sinks are files, Delta tables and Kafka), an **output mode** and a **trigger**.

`availableNow=True` processes all data available now and stops — perfect for scheduled "incremental batch" jobs and for reproducible notes. Without a trigger the query runs forever, starting a micro-batch as soon as the previous one finishes.'''),

    dict(t='13.3 Process only the new files: checkpoints',
         code='''def run_to_parquet():
    q = (orders.writeStream.format("parquet")
            .option("path", "data/stream/out/orders")
            .option("checkpointLocation", "data/stream/chk/orders")
            .trigger(availableNow=True).start())
    q.awaitTermination()
    return q.lastProgress["numInputRows"]

print("run 1 read", run_to_parquet(), "rows")
drop_file("batch2", [{"order_id": 5, "ts": "2026-03-01 10:31:00", "city": "Delhi", "amount": 700}])
print("run 2 read", run_to_parquet(), "rows")
print("run 3 read", run_to_parquet(), "rows")
print("table now has", spark.read.parquet("data/stream/out/orders").count(), "rows")''',
         expl='''The **checkpoint** folder records which files were already processed (and, for stateful queries, the running state). Run 1 reads the 4 orders of batch1. Run 2 reads **only** the new file. Run 3 finds nothing new.

That is how a scheduled streaming job gets **exactly-once** results: restart it as often as you like, and each input file is processed once. Never share a checkpoint between two queries.'''),

    dict(t='13.4 A running aggregate: outputMode("complete")',
         code='''per_city = orders.groupBy("city").agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))

q = (per_city.writeStream.format("memory").queryName("city_totals")
        .outputMode("complete").trigger(availableNow=True).start())
q.awaitTermination()
spark.sql("SELECT * FROM city_totals ORDER BY city").show()''',
         expl='''An aggregation keeps **state** — the running count and sum per city — between micro-batches. `complete` mode rewrites the whole result table after each batch.

`append` is not allowed for an aggregation without a watermark, because a city's total can still change when new orders arrive; Spark would refuse to start the query.'''),

    dict(t='13.5 Count per 10-minute window (event time)',
         code='''windowed = (orders
    .groupBy(F.window("ts", "10 minutes"), "city")
    .agg(F.count("*").alias("orders")))

q = (windowed.writeStream.format("memory").queryName("per_window")
        .outputMode("complete").trigger(availableNow=True).start())
q.awaitTermination()
spark.sql("SELECT window.start, window.end, city, orders FROM per_window ORDER BY start, city").show()''',
         expl='''`F.window(timeColumn, "10 minutes")` assigns each order to a 10-minute bucket by its **event time** — the `ts` inside the record — not by when Spark happened to read it. That keeps results correct when data arrives late or out of order.

`window("ts", "10 minutes", "5 minutes")` would make **sliding** windows that overlap.'''),

    dict(t='13.6 Late data and watermarks',
         code='''late_orders = spark.readStream.schema(schema).json("data/stream/in")
wm = (late_orders.withWatermark("ts", "10 minutes")
        .groupBy(F.window("ts", "10 minutes")).agg(F.count("*").alias("orders")))

def run_wm():
    q = (wm.writeStream.format("parquet")
           .option("path", "data/stream/out/wm").option("checkpointLocation", "data/stream/chk/wm")
           .outputMode("append").trigger(availableNow=True).start())
    q.awaitTermination()
    return q.lastProgress["eventTime"].get("watermark")

print("watermark after run 1:", run_wm())
drop_file("batch3", [{"order_id": 6, "ts": "2026-03-01 10:02:00", "city": "Pune", "amount": 450},   # 30 min late
                     {"order_id": 7, "ts": "2026-03-01 10:45:00", "city": "Pune", "amount": 650}])
print("watermark after run 2:", run_wm())
spark.read.parquet("data/stream/out/wm").orderBy("window").show(truncate=False)''',
         expl='''A **watermark** of 10 minutes says: "events more than 10 minutes older than the latest event seen can be ignored". After batch2 the latest event is 10:31, so the watermark is 10:21 — windows ending before it are **final**, written once in `append` mode, and their state is dropped.

Order 6 (10:02) arrives after its window was finalised, so it is **not counted**: the 10:00–10:10 window still shows 3 orders. Without a watermark, Spark would have to keep every window's state forever.'''),

    dict(t='13.7 Write each micro-batch with batch code: foreachBatch',
         code='''def save_batch(batch_df, batch_id):
    print(f"batch {batch_id}: {batch_df.count()} rows")
    (batch_df.withColumn("batch_id", F.lit(batch_id))
             .write.mode("append").parquet("data/stream/out/by_batch"))

one_file_each = (spark.readStream.schema(schema)
                      .option("maxFilesPerTrigger", 1)        # a source option
                      .json("data/stream/in"))

q = (one_file_each.writeStream.foreachBatch(save_batch)
        .option("checkpointLocation", "data/stream/chk/by_batch")
        .trigger(availableNow=True).start())
q.awaitTermination()''',
         expl='''`foreachBatch(func)` calls your function with each micro-batch as an ordinary **batch DataFrame** plus its batch ID. Inside, you can use any batch API: write to several sinks, MERGE into a Delta table, call an API, write to a JDBC database.

`maxFilesPerTrigger=1` — an option of the **source**, set on `readStream` — makes each file its own micro-batch, so the three files produced three batches (4, 1 and 2 orders). `availableNow` respects such limits and simply runs as many batches as needed. Use `batch_id` to make writes idempotent if a batch is retried.'''),

    dict(t='13.8 Enrich a stream with a static table (stream–static join)',
         code='''cities = spark.createDataFrame([("Pune", "West"), ("Mumbai", "West"), ("Delhi", "North")],
                               "city STRING, region STRING")

enriched = orders.join(cities, "city", "left")
q = (enriched.writeStream.format("memory").queryName("enriched")
        .outputMode("append").trigger(availableNow=True).start())
q.awaitTermination()
spark.sql("SELECT order_id, city, region, amount FROM enriched ORDER BY order_id").show()''',
         expl='''Joining a stream with a **static** DataFrame needs no state: each new order is looked up in the static table. The static side is re-read for every micro-batch, so updates to a dimension file are picked up.

Stream–stream joins are also supported but need watermarks on both sides to bound the state.'''),

    dict(t='13.9 Monitor a query: status and progress',
         code='''q = (orders.writeStream.format("memory").queryName("monitor")
        .trigger(availableNow=True).start())
q.awaitTermination()

p = q.lastProgress
print({k: p[k] for k in ["batchId", "numInputRows", "name"]})
print(p["sources"][0]["description"])
print(q.status["message"], "| active:", q.isActive)''',
         expl='''`lastProgress` (and `recentProgress`) report every micro-batch: rows read, input and processing rates, durations, watermark and state size. `status` says what the query is doing right now.

In production these numbers go to monitoring, and the Spark UI has a **Structured Streaming** tab with the same metrics as charts.'''),
]

POINTS = '''
1. A stream is an **unbounded table**; Spark runs your query incrementally in **micro-batches**.
2. `spark.readStream…` → transformations → `writeStream.format(sink).outputMode(mode).trigger(…).start()`.
3. File streams need an explicit **schema**. Upstream must write files atomically (write, then rename).
4. **Checkpoints** store progress and state → exactly-once results across restarts. One checkpoint per query.

    | Output mode | Writes | Allowed with |
    |---|---|---|
    | `append` | only new final rows | no aggregation, or aggregation + watermark |
    | `update` | rows that changed in this batch | aggregations |
    | `complete` | the whole result table | aggregations only |

    | Trigger | Behaviour |
    |---|---|
    | default | next micro-batch as soon as the last one ends |
    | `processingTime="1 minute"` | one micro-batch per minute |
    | `availableNow=True` | process everything available, then stop |

5. Group by **event time** with `F.window(ts, "10 minutes")`; bound state and late data with `withWatermark(ts, "10 minutes")`.
6. `foreachBatch` lets you use the full batch API (Delta MERGE, JDBC, several sinks) on each micro-batch.
7. Monitor with `lastProgress`, `status` and the Structured Streaming tab of the Spark UI.
'''

EXTRA_TITLE = 'Deep dive: streaming in production'
EXTRA_LEAD = 'Kafka, the errors you will meet first, and how exactly-once really works.'
EXTRA = [
    dict(t='Reading from Kafka', run=False,
         code='''events = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
    .select(F.from_json(F.col("value").cast("string"), schema).alias("o"))
    .select("o.*"))

(events.writeStream.format("parquet")
    .option("path", "s3://lake/orders").option("checkpointLocation", "s3://lake/_chk/orders")
    .trigger(processingTime="1 minute").start())''',
         expl='''Not run in these notes (it needs a Kafka cluster and the `spark-sql-kafka` package). Kafka delivers `key` and `value` as **bytes**: cast `value` to string and parse it with `from_json` (Chapter 8).

The checkpoint stores the Kafka **offsets** processed; together with an idempotent or transactional sink (file sink, Delta) that gives end-to-end exactly-once.'''),
    dict(t='Error: a streaming DataFrame cannot show()', error=True,
         code='''spark.readStream.schema(schema).json("data/stream/in").show()''',
         expl='''`show`, `count` and `collect` would need the whole, never-ending table. Start a query with `writeStream` instead — to the `memory` sink when you just want to look at results while developing.'''),
    dict(t='Error: append mode on an aggregation without a watermark', error=True,
         code='''(spark.readStream.schema(schema).json("data/stream/in")
      .groupBy("city").count()
      .writeStream.format("memory").queryName("bad").outputMode("append").start())''',
         expl='''In `append` mode a row may be written only once, when it is final. A per-city count is never final in an endless stream, so Spark refuses at start-up. Use `complete` or `update`, or aggregate by event-time window with a watermark.'''),
]

MCQS = [
    dict(q='What does the checkpoint location of a streaming query store?', kind='concept',
         o=['A copy of all input data', 'The progress (offsets / processed files) and the state of aggregations', 'The Spark UI logs', 'Only the schema'], a=1,
         why='Checkpoints record which input was processed and the operator state, so a restarted query continues exactly where it stopped.'),
    dict(q='Which output mode is **not** allowed for `groupBy("city").count()` on a stream without a watermark?', kind='concept',
         o=['complete', 'update', 'append', 'All are allowed'], a=2,
         why='append writes each row once when final; an unbounded per-city count is never final.'),
    dict(q='A 10-minute watermark is set and the latest event seen is 12:30. An event stamped 12:05 arrives. What happens to it in a 10-minute windowed count?', kind='concept',
         o=['It is counted in the 12:00–12:10 window', 'It is dropped because its window ended before the watermark (12:20)', 'The query fails', 'It is counted in the 12:30 window'], a=1,
         why='Watermark = 12:30 − 10 min = 12:20. The 12:00–12:10 window is already finalised, so the late event is ignored.'),
    dict(q='What does `trigger(availableNow=True)` do?', kind='concept',
         o=['Runs one micro-batch every second', 'Processes all currently available data (possibly in several batches), then stops', 'Runs forever with minimal latency', 'Reads only the newest file'], a=1,
         why='availableNow is designed for scheduled incremental jobs: catch up, then stop.'),
    dict(q='Why must a producer write a file under a temporary name and then rename it into the watched folder?', kind='concept',
         o=['Spark only reads files ending in .tmp', 'So the stream never reads a half-written file', 'Renaming compresses the file', 'It is required by JSON'], a=1,
         why='A rename is atomic: the file appears complete or not at all.'),
    dict(q='The code below raises an error. Why?', kind='debug',
         code='''stream = spark.readStream.json("data/stream/in")''',
         o=['json is not a streaming format', 'File streams need an explicit schema', 'The folder must be on HDFS', 'readStream needs .load()'], a=1,
         why='Spark cannot infer a schema from data that has not arrived yet; pass .schema(...).'),
    dict(q='Which API lets you MERGE each micro-batch into a Delta table or write it to a JDBC database?', kind='concept',
         o=['`foreach`', '`foreachBatch`', '`mapInPandas`', '`writeStream.format("jdbc")`'], a=1,
         why='foreachBatch gives you each micro-batch as a normal DataFrame, so any batch writer works.'),
]

PRACTICE = dict(
    kinds=['Re-create this chapter with your own folder of JSON files; add files one by one and re-run with availableNow.',
           'Windowed counts with a watermark — predict which late events are dropped, then check.',
           'A foreachBatch sink that writes to Parquet and also prints per-batch row counts.',
           'If you can run Docker: start a single Kafka broker and replace the file source with the Kafka source.'],
    note='Streaming is learned by watching state change across runs; there is no LeetCode equivalent.')
