ORDER = 0
KEY = '0'
TITLE = 'Spark Theory: why it exists, how it runs, where it fits'
WEEK = 'Week 1'

SECTIONS = [
    dict(t='0.1 The problem: data that no longer fits on one machine',
         md='''| | Scale up (one bigger machine) | Scale out (many ordinary machines) |
|---|---|---|
| How | More RAM, more cores, faster disk | Add machines to a cluster |
| Limit | The biggest machine you can buy | Practically none |
| Cost | Rises steeply at the high end | Grows roughly in line with size |
| Failure | One machine down = everything down | A machine down = its work is redone elsewhere |
| Examples | pandas, a single PostgreSQL server | Hadoop MapReduce, **Spark** |''',
         expl='''A laptop holds a few GB in memory; a company's click logs, orders and sensor data reach terabytes per day. Past a point no single machine can store or process that in time.

Spark's answer is to **split the data into pieces, process the pieces in parallel on many machines, and combine the results** — while hiding most of that complexity behind a DataFrame API that looks like pandas and SQL.'''),

    dict(t='0.2 From MapReduce to Spark',
         md='''| Year | Milestone |
|---|---|
| 2004 | Google publishes the MapReduce paper: map over pieces, shuffle by key, reduce |
| 2006 | Hadoop brings MapReduce and HDFS (distributed storage) to everyone |
| 2009 | Spark starts at UC Berkeley's AMPLab: keep data **in memory** between steps |
| 2014 | Spark becomes a top-level Apache project; Spark 1.0 |
| 2016 | Spark 2.0: **SparkSession**, DataFrames everywhere, Structured Streaming |
| 2020 | Spark 3.0: **Adaptive Query Execution**, faster Python (Arrow) |
| 2025 | Spark 4.0: **ANSI SQL mode by default**, Spark Connect, Python data sources |''',
         expl='''MapReduce wrote every intermediate result to disk, so a 10-step job read and wrote the data 10 times. Iterative work (machine learning, graph algorithms, interactive analysis) was painfully slow.

Spark keeps intermediate data in memory and plans the **whole chain of steps** before running it. These notes use Spark 4.2.'''),

    dict(t='0.3 Why Spark is fast',
         md='''| Idea | What it means | Where you will see it |
|---|---|---|
| **Parallelism** | Data is split into partitions; each core works on one | Chapter 1, Chapter 10 |
| **In-memory processing** | Steps pass data in memory instead of through disk | `cache()`, pipelined stages |
| **Lazy evaluation** | Nothing runs until you ask for a result, so Spark sees the whole plan | Chapter 1 |
| **Query optimiser (Catalyst)** | Rewrites your plan: pushes filters down, prunes columns, picks join strategies | `explain()` everywhere |
| **Code generation (Tungsten)** | Turns a chain of steps into compact Java bytecode | `*(1)` in plans |
| **Columnar files** | Reads only the columns and row groups a query needs | Parquet, Delta — Chapter 3, Chapter 12 |'''),

    dict(t='0.4 One engine, many workloads',
         md='''| Library | Use it for | Chapter |
|---|---|---|
| **Spark SQL / DataFrames** | Batch ETL, reporting, ad-hoc analysis — the core of this course | 1–11 |
| **Structured Streaming** | Continuous pipelines from Kafka, files, event hubs | 13 |
| **MLlib** | Distributed feature engineering and model training | 16 |
| **pandas API on Spark** | pandas syntax on big data | 11 |
| **Spark Connect** | Thin clients that talk to a remote Spark cluster | — |''',
         expl='''The same DataFrame, the same optimiser and the same cluster serve all of them. You can read a stream, join it with a batch table, score it with a model and write the result — in one program and one language.

Spark has APIs in Python, SQL, Scala, Java and R. **PySpark** is the Python API; most of the real work still runs inside Spark's JVM engine.'''),

    dict(t='0.5 The architecture: driver, executors, cluster manager',
         md='''| Part | What it does | On your laptop (`local[4]`) |
|---|---|---|
| **Driver** | Runs your program, builds the plan, splits it into tasks, collects results | Your Python process + a JVM |
| **Executors** | Processes on worker machines that run tasks and hold cached data | Threads inside the same JVM |
| **Cluster manager** | Hands out machines, cores and memory | Not needed |
| **Task** | One unit of work on one partition of data | One thread |''',
         expl='''On a real cluster the cluster manager is **YARN**, **Kubernetes** or **Spark Standalone**, and each executor is a separate JVM on a worker machine. On Databricks, EMR or Dataproc the platform runs the cluster manager for you.

The driver never touches the data itself unless you bring it back with `collect()` or `toPandas()` — then the whole result must fit in the driver's memory.''',
         tip='PySpark is a thin Python layer: your DataFrame calls are sent to the JVM through Py4J, and the JVM does the real work. That is why built-in functions are fast and Python UDFs are slow (Chapter 11).'),

    dict(t='0.6 Partitions, tasks and shuffles',
         md='''| Term | Meaning |
|---|---|
| **Partition** | A chunk of the rows. A 10 GB file might become 80 partitions of 128 MB |
| **Task** | Processing one partition through a sequence of steps; one core runs one task at a time |
| **Narrow transformation** | Each output partition depends on one input partition: `filter`, `select`, `withColumn` |
| **Wide transformation** | Rows must move between machines so equal keys meet: `groupBy`, `join`, `orderBy`, `distinct` |
| **Shuffle** | That movement: data is written, sent over the network and read again — the expensive part |
| **Stage** | A run of narrow steps between two shuffles |
| **Job** | All the stages needed to answer one action |''',
         expl='''Most of Spark performance work is about **shuffles**: avoiding unnecessary ones, making them smaller, and keeping their partitions evenly sized. Chapter 10 does this with real plans.'''),

    dict(t='0.7 Lazy evaluation: transformations plan, actions run',
         md='''| You write | What Spark does |
|---|---|
| `df = spark.read.parquet(...)` | Reads only the schema and file list |
| `df2 = df.filter(...).select(...)` | Adds steps to a plan — no data is read |
| `df3 = df2.groupBy(...).agg(...)` | Adds more steps — still nothing runs |
| `df3.show()` / `count()` / `write` | **Action**: Catalyst optimises the whole plan, splits it into stages and tasks, and runs it |''',
         expl='''Because nothing runs until the action, Spark can, for example, read only the 2 columns used out of 50, and apply a filter written last as early as the file scan. You write the logic in the order that is clear to you; Spark reorders it for speed.'''),

    dict(t='0.8 RDD, DataFrame, Dataset',
         md='''| API | What it is | Optimised by Catalyst? | In PySpark |
|---|---|---|---|
| **RDD** | A distributed collection of objects, transformed with functions (`map`, `filter`, `reduceByKey`) | No — functions are opaque | Available, legacy; avoid for new code |
| **DataFrame** | A distributed table with named, typed columns | Yes | **The API you will use** |
| **Dataset** | A typed DataFrame bound to a class | Yes | Scala/Java only |''',
         expl='''A DataFrame is like a pandas DataFrame or a SQL table, but split across machines and evaluated lazily. Everything in Chapters 1–16 uses DataFrames (and SQL, which compiles to the same plans).'''),

    dict(t='0.9 Spark, pandas and PostgreSQL side by side',
         md='''| | pandas | PostgreSQL | Spark |
|---|---|---|---|
| Runs on | One machine, in memory | One server (plus replicas) | Many machines |
| Data size | Up to RAM | Up to one server's disk | Terabytes to petabytes |
| Evaluation | Eager — every line runs now | Per query | Lazy — runs on an action |
| Storage | None of its own | Its own tables | Files you own: Parquet, Delta, CSV |
| Transactions | No | Full ACID, row-level | Via table formats such as Delta Lake |
| Best for | Small analysis, plotting | Applications (OLTP), many small reads/writes | Batch ETL, big aggregations, streaming, ML features |''',
         expl='''These tools complement each other: an application writes orders to PostgreSQL, Spark processes the history of all orders into analytics tables, and an analyst pulls a small aggregated result into pandas to plot it.'''),

    dict(t='0.10 Where Spark fits in a data platform',
         md='''| Layer | Typical tools | Spark's role |
|---|---|---|
| **Sources** | App databases (PostgreSQL), event streams (Kafka), files, APIs | Reads from all of them |
| **Ingestion** | Kafka, Fivetran/Airbyte, file drops, CDC | Structured Streaming or batch reads |
| **Storage (the lake / lakehouse)** | S3, ADLS, GCS with Parquet, **Delta Lake**, Iceberg | Writes and maintains the tables |
| **Processing** | **Spark**, dbt, Flink | Cleans, joins and aggregates |
| **Orchestration** | Airflow, Databricks Workflows | Is the job being scheduled |
| **Serving** | BI tools (Power BI, Tableau), ML models, APIs | Produces the tables they read |''',
         expl='''Spark is the **processing engine** in the middle. It does not store data itself — it reads files and tables from cheap object storage and writes new ones back. That separation of storage and compute is why a lake can hold petabytes and still scale its processing up and down on demand.'''),

    dict(t='0.11 Preview: the medallion architecture (Bronze → Silver → Gold)',
         md='''| Layer | Holds | Built in this course |
|---|---|---|
| **Bronze** — raw | Data exactly as it arrived, plus load metadata; append-only | Chapter 3: reading and landing files |
| **Silver** — clean | Typed, deduplicated, validated, conformed data | Chapter 4: built-in functions for cleaning |
| **Gold** — business | Aggregated tables for reports, dashboards and ML | Chapter 5: aggregations |
| All three, properly | Delta tables with MERGE, SCD2 and incremental loads | Chapter 12 |
| End to end | The ShopKart pipeline, Bronze → Silver → Gold | Chapter 15 capstone |''',
         expl='''Most lakehouses organise tables in these three layers. Keeping the raw Bronze copy means you can always rebuild Silver and Gold when a cleaning rule changes or a bug is found — nothing upstream has to be re-sent.

Watch for the layer labels in Chapters 3, 4 and 5: each one ends by writing its layer.'''),
]

POINTS = '''
1. Spark **scales out**: data is split into partitions processed in parallel across a cluster.
2. It improved on MapReduce by keeping data **in memory** and optimising the **whole plan** before running it.
3. **Driver** plans and coordinates; **executors** run tasks; the **cluster manager** (YARN, Kubernetes, Standalone) provides the machines.
4. Partition → task → stage → job. **Shuffles** (wide transformations) split stages and dominate cost.
5. **Lazy evaluation**: transformations build a plan, **actions** (`show`, `count`, `collect`, `write`) run it.
6. Use **DataFrames** (and SQL) in PySpark; RDDs are legacy.
7. Spark is the **processing** layer of a lakehouse: it reads and writes files on object storage, it is not a database.
8. **Medallion**: Bronze (raw) → Silver (clean) → Gold (business-ready).
'''

MCQS = [
    dict(q='What is the main reason Spark is faster than classic Hadoop MapReduce for multi-step jobs?', kind='concept',
         o=['It is written in Python', 'It keeps intermediate data in memory and optimises the whole plan, instead of writing to disk after every step', 'It uses fewer machines', 'It does not shuffle data'], a=1,
         why='MapReduce writes intermediate results to disk between steps; Spark pipelines steps in memory and plans the whole job first.'),
    dict(q='Which component turns your program into tasks and collects the results?', kind='concept',
         o=['Executor', 'Driver', 'Cluster manager', 'Partition'], a=1,
         why='The driver runs your code, builds the plan, schedules tasks on executors and receives results.'),
    dict(q='Which of these is a **wide** transformation that causes a shuffle?', kind='concept',
         o=['`filter`', '`select`', '`groupBy(...).agg(...)`', '`withColumn`'], a=2,
         why='Grouping must bring all rows with the same key together, which moves data between machines.'),
    dict(q='A job reads a 50-column table, filters it and shows 2 columns. Because of lazy evaluation, Spark can…', kind='concept',
         o=['Only read the 2 needed columns and apply the filter during the scan', 'Run each line immediately as you type it', 'Skip the filter', 'Cache the whole table automatically'], a=0,
         why='Seeing the whole plan before running lets Catalyst prune columns and push filters into the scan.'),
    dict(q='In a lakehouse, what is Spark\'s main role?', kind='concept',
         o=['Storing the data permanently', 'The processing engine that reads, transforms and writes tables on storage', 'A BI dashboard tool', 'A replacement for the application database'], a=1,
         why='Storage is object storage with Parquet/Delta files; Spark is the compute that processes them.'),
    dict(q='Which medallion layer keeps data exactly as it arrived?', kind='concept',
         o=['Gold', 'Silver', 'Bronze', 'Platinum'], a=2,
         why='Bronze is the raw, append-only landing layer. Silver is cleaned; Gold is business-level aggregates.'),
    dict(q='Which API should new PySpark code use?', kind='concept',
         o=['RDD', 'DataFrame (and Spark SQL)', 'Dataset', 'MapReduce'], a=1,
         why='DataFrames are optimised by Catalyst; Datasets are not available in Python; RDDs are legacy.'),
]

PRACTICE = dict(
    kinds=['Explain to a classmate, without slides, what happens between `df.count()` and the number appearing.',
           'Draw the data platform of a company you know (a food-delivery or e-commerce app): sources, storage, Spark jobs, dashboards.',
           'For five operations you use often, say narrow or wide — then check with explain() in Chapter 1.'],
    note='This chapter is theory. The code starts in Chapter 1.')
