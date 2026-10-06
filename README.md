# PySpark Class Notes — Presentation

A presentation-style website of PySpark class notes, from the first SparkSession to Structured Streaming, MLlib and production pipelines. It is built in the same design as the [PostgreSQL Class Notes](https://edusatyaki.github.io/PostgreSQLNotes/).

## What is inside

- **14 chapters + a one-page SQL ↔ PySpark quick reference**, following the course syllabus: SparkSession and architecture, DataFrame basics, built-in functions, aggregations, complex types and JSON, joins, window functions, Spark SQL, reading and writing data, performance and internals, UDFs and pandas, Structured Streaming, MLlib, and production PySpark.
- **192 sections**, each a question with the PySpark code, the **real output from Spark 4.2**, and an explanation. The DataFrame a query works on is shown beside it.
- **44 deep-dive slides** on internals: plans, shuffles, broadcast vs sort-merge joins, AQE, skew, memory, Parquet pushdown and streaming state.
- **114 MCQs**: concept questions, predict-the-output questions (run on Spark when the site is built), and debug-the-code questions.
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

Requirements: Java 17 and Python 3.13 with the packages in `requirements.txt`.

```
python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh --run        # execute everything on Spark and render (about a minute)
./run.sh --run 7      # re-run one chapter only
./run.sh              # render from the saved outputs
```

The build fails if a code block raises unexpectedly, if a block marked `error=True` does not raise, or if a predict-the-output MCQ's answer does not appear in the real output.

---

© Satyaki Das · [GitHub](https://github.com/edusatyaki) · [LinkedIn](https://www.linkedin.com/in/satyaki-das-25593714a/)
