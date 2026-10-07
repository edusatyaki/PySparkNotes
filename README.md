# PySpark Class Notes — Presentation

A presentation-style website of PySpark class notes, from the first SparkSession to Structured Streaming, MLlib and production pipelines. It is built in the same design as the [PostgreSQL Class Notes](https://edusatyaki.github.io/PostgreSQLNotes/).

## What is inside

The chapters follow the course sequence:

| # | Chapter |
|---|---|
| 0 | Spark theory — why Spark exists, how it runs, where it fits, medallion preview |
| 1 | Getting started — SparkSession, partitions, lazy evaluation in code |
| 2 | DataFrame basics + `spark.sql` |
| 3 | Reading and writing basics — raw landing = **Bronze** |
| 4 | Built-in functions — cleaning = **Silver** |
| 5 | Aggregations — business tables = **Gold** |
| 6 | Joins |
| 7 | Window functions |
| 8 | Complex types and JSON |
| 9 | Spark SQL and the catalog |
| 10 | Performance and internals |
| 11 | UDFs and pandas |
| 12 | Delta Lake + the medallion architecture — MERGE, SCD2, incremental loads, CDF, RESTORE |
| 13 | Structured Streaming |
| 14 | Production PySpark — structure, tests, data quality, idempotent writes |
| 15 | Capstone: the ShopKart pipeline, Bronze → Silver → Gold on Delta |
| 16 | MLlib (optional) |
| 17 | Quick reference — the whole course on one page |

- **245 sections**, each a question with the PySpark code, the **real output from Spark 4.2** (Delta Lake 4.4 for chapters 12 and 15), and an explanation. The DataFrame a query works on is shown beside it.
- **40 deep-dive slides** on internals: plans, shuffles, joins, AQE, skew, memory, Parquet pushdown, streaming state.
- **134 MCQs**: concept, predict-the-output (run on Spark at build time) and debug-the-code questions.
- **Practice after every chapter**, with LeetCode problems to solve with the DataFrame API.

It has the same interactive features as the PostgreSQL notes: home page with a live terminal, chapter menu with search, mark for revisit, predict mode (**P**), rapid-fire quiz (**Q**), confetti, per-chapter progress, paper page turn, and a scrollable reading view on phones.

## How it is built

`index.html` is **generated**. Never edit it by hand.

```
content/chNN.py        one module per chapter: setup code, sections, points, deep dives, MCQs, practice
build/build.py         runs every code block on real Spark, then renders index.html
build/template/        page shell, CSS and slide engine (shared design with PostgreSQLNotes)
build/outputs.json     the captured Spark outputs (committed, so rendering needs no Spark)
```

Requirements: Java 17 and Python 3.13 with the packages in `requirements.txt`. The first run downloads the Delta Lake JARs from Maven Central.

```
python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh --run        # execute everything on Spark and render (about a minute)
./run.sh --run 7      # re-run one chapter only
./run.sh              # render from the saved outputs
```

The build fails if a code block raises unexpectedly, if a block marked `error=True` does not raise, or if a predict-the-output MCQ's answer does not appear in the real output.

---

© Satyaki Das · [GitHub](https://github.com/edusatyaki) · [LinkedIn](https://www.linkedin.com/in/satyaki-das-25593714a/)
