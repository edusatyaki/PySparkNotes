ORDER = 11
KEY = '11'
TITLE = 'UDFs, pandas UDFs and the pandas API on Spark'
WEEK = 'Week 6'
DATA = ['reviews']

SETUP_TITLE = '11.0 The data — product reviews'
SETUP = '''import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, pandas_udf

reviews = spark.createDataFrame([
    (1, "Phone",      "Great battery, fast delivery",      5, 18000.0),
    (2, "Phone",      "screen cracked in a week",          1, 18000.0),
    (3, "Headphones", "Good bass. Good value!",            4,  1500.0),
    (4, "Headphones", None,                                3,  1500.0),
    (5, "Charger",    "works",                             4,   600.0),
    (6, "Charger",    "Stopped working, refund please",    2,   650.0),
], "review_id INT, product STRING, text STRING, rating INT, price DOUBLE")'''
SETUP_EXPL = '''Six reviews, one with a NULL text. A **UDF** (user-defined function) lets you run your own Python code on each row when no built-in function does the job.

The price: each row must be sent from the JVM to a Python worker process and the result sent back. This chapter shows how to make that cost small — and when to avoid it entirely.'''

SECTIONS = [
    dict(t='11.1 A Python UDF — and the NULL that breaks it', tables=['reviews'], error=True,
         code='''@udf("int")
def word_count(text):
    return len(text.split())

reviews.select("review_id", word_count("text").alias("words")).show()''',
         expl='''`@udf(returnType)` wraps a plain Python function so it can be used on columns. The return type is required; if the function returns something else, you silently get NULL.

The job fails on review 4: the UDF receives Python `None` for a NULL and `None.split()` raises. Built-in functions handle NULL for you; **your UDF must handle it itself**.'''),

    dict(t='11.2 The same UDF, NULL-safe', tables=['reviews'],
         code='''@udf("int")
def word_count(text):
    if text is None:
        return None
    return len(text.split())

reviews.select("review_id", "text", word_count("text").alias("words")).show(truncate=False)''',
         expl='''Return `None` for a NULL input — that keeps SQL semantics (NULL in, NULL out). Every Python UDF should start with this check.'''),

    dict(t='11.3 Was the UDF needed? The built-in version', tables=['reviews'],
         code='''builtin = F.size(F.split(F.trim("text"), r"\\s+"))
reviews.select("review_id", builtin.alias("words")).show()

reviews.select(word_count("text")).explain()
reviews.select(builtin).explain()''',
         expl='''The built-in expression gives the same counts (NULL text → NULL) and its plan is a plain `Project` that runs inside the JVM with code generation.

The UDF plan has an extra **`ArrowEvalPython`** step: rows are batched, shipped to Python and back. **Always look for a built-in first** — `F.` has over 400 functions.'''),

    dict(t='11.4 Use a UDF from SQL: spark.udf.register', tables=['reviews'],
         code='''def sentiment(text):
    if text is None:
        return "unknown"
    t = text.lower()
    if any(w in t for w in ["great", "good", "works"]):
        return "positive"
    if any(w in t for w in ["cracked", "stopped", "refund"]):
        return "negative"
    return "neutral"

spark.udf.register("sentiment", sentiment, "string")
reviews.createOrReplaceTempView("reviews")
spark.sql("SELECT review_id, rating, sentiment(text) AS mood FROM reviews").show()''',
         expl='''`spark.udf.register(name, function, returnType)` makes a Python function callable from SQL. It is still a Python UDF with the same cost.

Real sentiment analysis would call a model; the pattern is the same — and that is a case where Python really is needed.'''),

    dict(t='11.5 Return several values at once: a struct UDF', tables=['reviews'],
         code='''@udf("struct<words: int, exclaims: int, upper_start: boolean>")
def text_stats(text):
    if text is None:
        return None
    return (len(text.split()), text.count("!"), text[:1].isupper())

(reviews.withColumn("s", text_stats("text"))
        .select("review_id", "s.words", "s.exclaims", "s.upper_start").show())''',
         expl='''Declare a **struct** return type and return a tuple (or dict) — one UDF call computes several fields, instead of three UDFs each paying the transfer cost.'''),

    dict(t='11.6 Vectorised: a pandas UDF (Series → Series)', tables=['reviews'],
         code='''@pandas_udf("double")
def price_with_gst(price: pd.Series) -> pd.Series:
    return (price * 1.18).round(2)

reviews.select("product", "price", price_with_gst("price").alias("with_gst")).show()''',
         expl='''A **pandas UDF** receives a whole batch of rows as a `pandas.Series` (thousands at a time, transferred with Apache Arrow) and returns a Series of the same length. The Python function runs once per batch, not once per row, so it is typically **10–100× faster** than a row-at-a-time UDF.

The type hints `pd.Series -> pd.Series` tell Spark which kind of pandas UDF this is.'''),

    dict(t='11.7 A custom aggregate: pandas UDF Series → scalar', tables=['reviews'],
         code='''@pandas_udf("double")
def mean_rating(r: pd.Series) -> float:
    return float(r.mean())

@pandas_udf("double")
def price_weighted(r: pd.Series, p: pd.Series) -> float:
    return float((r * p).sum() / p.sum())

reviews.groupBy("product").agg(
    mean_rating("rating").alias("mean"),
    F.round(price_weighted("rating", "price"), 2).alias("weighted")).orderBy("product").show()''',
         expl='''With a `Series -> scalar` signature the pandas UDF becomes an **aggregate function** usable in `groupBy().agg()`. Each group's values arrive as one Series.

Here: the rating weighted by price, which takes **two** columns — each arrives as its own Series. Use this for statistics Spark does not have built in. Two limits: all values of a group must fit in one Python worker's memory, and a pandas aggregate **cannot be mixed with built-in aggregates** such as `F.avg` in the same `agg` (`INVALID_PANDAS_UDF_PLACEMENT`) — which is why the mean here is a pandas UDF too.'''),

    dict(t='11.8 Run pandas code per group: applyInPandas', tables=['reviews'],
         code='''def add_rating_z(pdf: pd.DataFrame) -> pd.DataFrame:
    std = pdf["rating"].std(ddof=0)
    pdf["rating_z"] = 0.0 if std == 0 else (pdf["rating"] - pdf["rating"].mean()) / std
    return pdf[["review_id", "product", "rating", "rating_z"]]

(reviews.groupBy("product")
        .applyInPandas(add_rating_z, schema="review_id INT, product STRING, rating INT, rating_z DOUBLE")
        .orderBy("review_id").show())''',
         expl='''`groupBy(...).applyInPandas(func, schema)` hands each group to your function as a **pandas DataFrame** and stitches the returned DataFrames together. Any pandas code works inside — including fitting a small model per group ("one forecast per store").

You must declare the output schema. Each group must fit in memory, so watch out for skewed keys.'''),

    dict(t='11.9 Transform partitions as pandas: mapInPandas', tables=['reviews'],
         code='''def flag_expensive(batches):
    for pdf in batches:
        pdf["expensive"] = pdf["price"] > 1000
        yield pdf[["review_id", "price", "expensive"]]

reviews.mapInPandas(flag_expensive, "review_id INT, price DOUBLE, expensive BOOLEAN").show()''',
         expl='''`mapInPandas` streams **batches** of each partition through a Python generator. No grouping and no shuffle; the number of output rows may differ from the input (you may filter or expand).

It is the right tool for loading a model once per partition and scoring every batch with it.'''),

    dict(t='11.10 One row in, many rows out: a Python UDTF',
         code='''from pyspark.sql.functions import udtf

@udtf(returnType="word: string, pos: int")
class SplitWords:
    def eval(self, text: str):
        if text:
            for i, w in enumerate(text.lower().replace(",", "").split()):
                yield w, i

spark.udtf.register("split_words", SplitWords)
spark.sql("SELECT r.review_id, s.word, s.pos FROM reviews r, LATERAL split_words(r.text) s WHERE r.review_id = 1").show()''',
         expl='''A **user-defined table function** (Spark 3.5+) returns a table: `eval` is called per input row and can `yield` any number of output rows. It is the Python counterpart of `explode`, usable from SQL with `LATERAL`.'''),

    dict(t='11.11 Spark ↔ pandas with Arrow', tables=['reviews'],
         code='''print(spark.conf.get("spark.sql.execution.arrow.pyspark.enabled"))

pdf = reviews.groupBy("product").agg(F.avg("rating").alias("avg_rating")).toPandas()
print(pdf.sort_values("product").to_string(index=False))

back = spark.createDataFrame(pdf)
back.printSchema()''',
         expl='''With Arrow enabled, `toPandas()` and `createDataFrame(pandas_df)` move data in columnar batches instead of row by row — much faster for anything beyond a few thousand rows.

Aggregate in Spark first, then `toPandas()` the small result for plotting or a report.'''),

    dict(t='11.12 pandas syntax on Spark: pyspark.pandas', tables=['reviews'],
         code='''import pyspark.pandas as ps

psdf = reviews.pandas_api()                 # a pandas-on-Spark DataFrame
print(type(psdf).__module__)
print(psdf.groupby("product")["rating"].mean().sort_index())
print(psdf[psdf["price"] > 1000][["review_id", "product"]].to_spark().count())''',
         expl='''The **pandas API on Spark** (formerly Koalas) gives pandas syntax — `groupby`, boolean indexing, `sort_index` — executed by Spark on distributed data. `to_spark()` converts back.

Good for moving existing pandas code to bigger data. Not every pandas feature is supported, and operations that need a global row order (like a default index) can be expensive.'''),
]

POINTS = '''
1. Look for a **built-in** first: built-ins run in the JVM, get code generation and filter pushdown.
2. Python UDF: `@udf(returnType)`; **handle `None`** yourself; a wrong return type is never an error — it silently becomes NULL (classic UDF) or is cast (Arrow UDF).
3. Return a **struct** to compute several values in one UDF call.
<!--break-->
4. pandas UDFs move batches with Arrow and run once per batch:

    | Tool | Input → output | Use for |
    |---|---|---|
    | `@udf` | value → value | Small custom logic, no alternative |
    | `@pandas_udf` Series → Series | column batch → column batch | Vectorised maths, string work |
    | `@pandas_udf` Series → scalar | group → one value | Custom aggregates |
    | `groupBy().applyInPandas` | group DataFrame → DataFrame | Per-group models, complex pandas code |
    | `mapInPandas` | batches → batches | Load a model once per partition, score |
    | `@udtf` | row → many rows | Python table functions in SQL |

5. `spark.udf.register` / `spark.udtf.register` make them callable from SQL.
6. `toPandas()` collects to the driver — only for small, aggregated results. Arrow makes it fast.
7. `df.pandas_api()` / `pyspark.pandas` = pandas syntax executed by Spark.
'''

EXTRA_TITLE = 'Deep dive: what a UDF really costs'
EXTRA_LEAD = 'Where the time goes, and the two settings that matter.'
EXTRA = [
    dict(t='The round trip of every Python UDF row',
         md='''| Step | Where | Cost |
|---|---|---|
| 1. Serialise rows from Spark's internal format | JVM executor | CPU |
| 2. Send them over a socket to a Python worker | Executor → Python process | I/O |
| 3. Deserialise into Python objects, call your function | Python worker | Interpreter speed |
| 4. Serialise the results and send them back | Python → JVM | I/O + CPU |
| 5. Optimiser cannot see inside the function | Catalyst | No pushdown, no reordering |''',
         expl='''Arrow batching (pandas UDFs, and Arrow-optimised regular UDFs in recent Spark versions) shrinks steps 1–4 dramatically. Step 5 never goes away — that is why a built-in filter can skip whole files and a UDF filter cannot.'''),
    dict(t='Arrow settings in this session',
         code='''for k in ["spark.sql.execution.arrow.pyspark.enabled",
          "spark.sql.execution.pythonUDF.arrow.enabled",
          "spark.sql.execution.arrow.maxRecordsPerBatch"]:
    print(f"{k:48} {spark.conf.get(k)}")''',
         expl='''`maxRecordsPerBatch` (10,000) is how many rows a pandas UDF receives per call. Lower it if each batch uses too much memory in Python; raise it for cheap per-row work.'''),
    dict(t='A wrong return type: a silent cast or a silent NULL', tables=['reviews'],
         code='''def half(p):
    return p / 2 + 0.25   # a float, but the UDFs below promise an int

arrow_udf = udf(half, "int")                     # Arrow-optimised (Spark 4.2 default)
pickle_udf = udf(half, "int", useArrow=False)    # classic row-by-row UDF

reviews.select("price", arrow_udf("price").alias("arrow"),
               pickle_udf("price").alias("pickled")).show(3)''',
         expl='''The function returns a `float` but both UDFs declare `int`. The Arrow-optimised UDF **casts** it (9000.25 → 9000, the fraction is lost); the classic pickled UDF writes **NULL**. Neither raises an error or a warning.

Declare the type the function really returns (`"double"`), and test every UDF on a few rows before running it on the full data.'''),
]

MCQS = [
    dict(q='Why is a Python UDF usually slower than an equivalent built-in function?', kind='concept',
         o=['Python UDFs run on the driver only', 'Rows are serialised to a Python worker and back, and the optimiser cannot see inside the function', 'UDFs disable caching', 'UDFs always cause a shuffle'], a=1,
         why='The JVM ↔ Python transfer and the loss of optimisations (pushdown, code generation) make UDFs slower.'),
    dict(q='A classic UDF, `udf(f, "int", useArrow=False)`, returns `3.5`. What appears in the column?', kind='concept',
         o=['3', '4', 'NULL', 'An error'], a=2,
         why='A pickled UDF writes NULL when the value does not match the declared type. (An Arrow-optimised UDF would cast it to 3 instead.) Either way, no error is raised.'),
    dict(q='Which pandas UDF signature defines a custom **aggregate** usable in `groupBy().agg()`?', kind='concept',
         o=['`pd.Series -> pd.Series`', '`pd.Series -> float`', '`pd.DataFrame -> pd.DataFrame`', '`Iterator[pd.Series] -> Iterator[pd.Series]`'], a=1,
         why='Series → scalar pandas UDFs are aggregate functions.'),
    dict(q='You must fit one small model per store and return predictions for that store\'s rows. Which API fits?', kind='concept',
         o=['A Python UDF', '`groupBy("store").applyInPandas(fit_and_predict, schema)`', '`mapInPandas`', '`F.transform`'], a=1,
         why='applyInPandas gives each group as a pandas DataFrame and returns a DataFrame — ideal for per-group models.'),
    dict(q='This UDF fails on some rows. What is the fix?', kind='debug',
         code='''@udf("string")
def clean(s):
    return s.strip().lower()''',
         o=['Declare the return type as "int"', 'Return None when s is None before calling methods on it', 'Use @pandas_udf instead', 'Call it inside F.lit'], a=1,
         why='NULL arrives as None; None.strip() raises. Check for None first.'),
    dict(q='What does this print?', kind='output', run=True, check='[2, 4, 6]',
         code='''@pandas_udf("long")
def double(s: pd.Series) -> pd.Series:
    return s * 2

print([r[0] for r in spark.range(1, 4).select(double("id")).collect()])''',
         o=['`[1, 2, 3]`', '`[2, 4, 6]`', '`[1, 4, 9]`', '`[0, 2, 4]`'], a=1,
         why='Each batch of ids (1, 2, 3) is doubled as a pandas Series.'),
    dict(q='Which statement about `df.toPandas()` is true?', kind='concept',
         o=['It keeps the data distributed', 'It collects all rows to the driver as a pandas DataFrame', 'It is a transformation', 'It only works with Arrow disabled'], a=1,
         why='toPandas is an action that brings every row to the driver. Use it only on small results.'),
]

PRACTICE = dict(
    kinds=['Rewrite every UDF you have written as built-ins where possible, and compare the plans.',
           'Turn a row-at-a-time UDF into a pandas UDF; measure the difference on 10 million rows from spark.range().',
           'Per-group pandas code with applyInPandas: normalise a metric per store, or fit a tiny regression per product.'],
    note='These are engineering skills rather than puzzle problems — practise them on generated data.')
