ORDER = 5
KEY = '5'
TITLE = 'Aggregations: groupBy, HAVING, pivot, rollup — business tables = Gold'
WEEK = 'Week 3'
DATA = ['sales']

SETUP_TITLE = '5.0 The data — one row per order line'
SETUP = '''from pyspark.sql import functions as F

sales = spark.createDataFrame([
    (1, "2026-01-05", "North", "Delhi",   "Electronics", "Headphones", 2, 1500.0),
    (2, "2026-01-07", "West",  "Mumbai",  "Fashion",     "Jeans",      1, 1200.0),
    (3, "2026-01-12", "North", "Delhi",   "Fashion",     "T-shirt",    3,  400.0),
    (4, "2026-01-20", "South", "Chennai", "Electronics", "Phone",      1, 18000.0),
    (5, "2026-02-02", "West",  "Pune",    "Home",        "Lamp",       2,  900.0),
    (6, "2026-02-03", "North", "Jaipur",  "Electronics", "Charger",    4,  600.0),
    (7, "2026-02-14", "South", "Bengaluru","Fashion",    "Jacket",     1, 3500.0),
    (8, "2026-02-18", "West",  "Mumbai",  "Electronics", "Phone",      1, 21000.0),
    (9, "2026-03-01", "East",  "Kolkata", "Home",        "Bedsheet",   2, 1100.0),
    (10,"2026-03-04", "South", "Chennai", "Home",        "Lamp",       1,  950.0),
    (11,"2026-03-09", "North", "Delhi",   "Electronics", "Headphones", 1, 1500.0),
    (12,"2026-03-15", "East",  "Kolkata", "Fashion",     "Saree",      1, 4500.0),
    (13,"2026-03-21", "West",  "Pune",    "Fashion",     "T-shirt",    5,  350.0),
    (14,"2026-03-28", "South", "Bengaluru","Electronics","Charger",    2,  650.0),
], "order_id INT, order_date STRING, region STRING, city STRING, "
   "category STRING, product STRING, qty INT, price DOUBLE") \\
   .withColumn("order_date", F.to_date("order_date")) \\
   .withColumn("amount", F.col("qty") * F.col("price"))'''
SETUP_EXPL = '''Fourteen order lines across four regions and three categories. `amount = qty × price` is computed once in the setup so every later query can use it.

Grouping is a **wide** transformation: rows with the same key must meet on one partition, so every `groupBy` adds a shuffle.'''

SECTIONS = [
    dict(t='5.1 Totals over the whole table (agg without groupBy)', tables=['sales'],
         code='''sales.agg(
    F.count("*").alias("orders"),
    F.sum("amount").alias("revenue"),
    F.round(F.avg("amount"), 2).alias("avg_order"),
    F.min("amount").alias("smallest"),
    F.max("amount").alias("largest"),
).show()''',
         expl='''`agg` without `groupBy` aggregates the whole DataFrame into **one row**. Give every aggregate an `alias`, otherwise you get names like `sum(amount)`.

`F.count("*")` counts rows; `F.count("col")` counts non-NULL values of that column.'''),

    dict(t='5.2 Revenue per region (groupBy + agg)', tables=['sales'], home=True,
         code='''(sales.groupBy("region")
      .agg(F.count("*").alias("orders"),
           F.sum("amount").alias("revenue"))
      .orderBy(F.desc("revenue"))
      .show())''',
         expl='''`groupBy(cols).agg(...)` returns one row per group. The output has no guaranteed order — add `orderBy` when order matters.

Shortcut methods exist (`groupBy("region").sum("amount")`), but `agg` lets you compute several aggregates with clean aliases in one pass.'''),

    dict(t='5.3 Group by two columns', tables=['sales'],
         code='''(sales.groupBy("region", "category")
      .agg(F.sum("qty").alias("units"), F.sum("amount").alias("revenue"))
      .orderBy("region", "category")
      .show())''',
         expl='''One output row per **combination** of region and category that actually occurs in the data. Combinations with no orders (East–Electronics) simply do not appear — `pivot` on the next slides shows them as NULL cells.'''),

    dict(t='5.4 Only the regions above ₹20,000 (HAVING)', tables=['sales'],
         code='''(sales.groupBy("region")
      .agg(F.sum("amount").alias("revenue"))
      .filter(F.col("revenue") > 20000)
      .show())''',
         expl='''There is no `having` method: a `filter` **after** `agg` is HAVING, because it runs on the aggregated rows. A `filter` **before** `groupBy` is WHERE — it removes order lines before they are summed.

Spark's optimiser places each filter correctly in the plan; you only need the right position in your code.'''),

    dict(t='5.5 Distinct counts: countDistinct vs approx_count_distinct', tables=['sales'],
         code='''sales.groupBy("region").agg(
    F.count("*").alias("lines"),
    F.countDistinct("product").alias("products"),
    F.approx_count_distinct("city").alias("cities_approx"),
).orderBy("region").show()''',
         expl='''`countDistinct` is exact but expensive on big data: Spark must keep every distinct value. `approx_count_distinct` uses the HyperLogLog++ sketch — a small fixed amount of memory, about 5% error by default, and exact on small data like this.

On billions of rows (daily active users, unique visitors), the approximate version is often 10× faster.'''),

    dict(t='5.6 Conditional aggregation: sum(when) and count_if', tables=['sales'],
         code='''sales.groupBy("region").agg(
    F.sum(F.when(F.col("category") == "Electronics", F.col("amount")).otherwise(0))
     .alias("electronics_rev"),
    F.count_if(F.col("amount") >= 5000).alias("big_orders"),
    F.round(F.avg(F.when(F.col("category") == "Fashion", F.col("amount"))), 1)
     .alias("avg_fashion"),
).orderBy("region").show()''',
         expl='''Putting `when` **inside** an aggregate filters per column instead of per query — several "filtered totals" in one pass. This is SQL's `SUM(CASE WHEN …)` / `COUNT(*) FILTER (WHERE …)`.

In the `avg` there is no `otherwise`, so non-Fashion rows become NULL and `avg` ignores them — the average is over Fashion orders only. With `otherwise(0)` the zeros would drag it down.'''),

    dict(t='5.7 Keep the values, not just the count: collect_list and collect_set', tables=['sales'],
         code='''(sales.groupBy("region")
      .agg(F.collect_list("product").alias("all_products"),
           F.array_sort(F.collect_set("category")).alias("categories"))
      .orderBy("region")
      .show(truncate=False))''',
         expl='''`collect_list` gathers every value into an array (duplicates kept, order not guaranteed); `collect_set` removes duplicates. Wrap with `array_sort` for a stable order.

Both build the whole array on one executor, so never `collect_list` a group with millions of values.'''),

    dict(t='5.8 Monthly revenue: group by a derived column', tables=['sales'],
         code='''(sales.groupBy(F.date_trunc("month", "order_date").cast("date").alias("month"))
      .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders"))
      .orderBy("month")
      .show())''',
         expl='''`groupBy` accepts expressions, not only column names. Truncating the date to the month start is the standard way to build a monthly time series.

`date_trunc` returns a timestamp; the `cast("date")` drops the `00:00:00`.'''),

    dict(t='5.9 Turn categories into columns (pivot)', tables=['sales'],
         code='''(sales.groupBy("region")
      .pivot("category", ["Electronics", "Fashion", "Home"])
      .agg(F.sum("amount"))
      .orderBy("region")
      .show())''',
         expl='''`pivot(col, values)` makes one output column per value. Listing the values is optional but recommended: without the list Spark runs an extra job to find the distinct values first.

Empty cells are NULL (East sold no Electronics). Add `.fillna(0)` for a report.'''),

    dict(t='5.10 Columns back into rows (unpivot)', tables=['sales'],
         code='''wide = (sales.groupBy("region").pivot("category", ["Electronics", "Fashion", "Home"])
             .agg(F.sum("amount")).fillna(0))

long = wide.unpivot("region", ["Electronics", "Fashion", "Home"], "category", "revenue")
long.orderBy("region", "category").show(6)''',
         expl='''`unpivot(ids, values, variableColumnName, valueColumnName)` (Spark 3.4+, also called `melt`) reverses a pivot: one row per region × category.

Long format is what you store and group; wide format is what you show in a report.'''),

    dict(t='5.11 Subtotals and a grand total (rollup)', tables=['sales'],
         code='''(sales.rollup("region", "category")
      .agg(F.sum("amount").alias("revenue"))
      .orderBy(F.col("region").asc_nulls_last(), F.col("category").asc_nulls_last())
      .show(25))''',
         expl='''`rollup(a, b)` computes `(a, b)`, then `(a)` subtotals, then the grand total `()`. NULL in a key column means "all values" — `North, NULL` is the North subtotal and `NULL, NULL` is the grand total.

`asc_nulls_last` puts each subtotal after its detail rows.'''),

    dict(t='5.12 Every combination of subtotals (cube) and grouping_id', tables=['sales'],
         code='''(sales.cube("region", "category")
      .agg(F.sum("amount").alias("revenue"), F.grouping_id().alias("gid"))
      .filter(F.col("gid") == 2)          # category subtotals only
      .orderBy("category")
      .show())''',
         expl='''`cube(a, b)` adds the subtotals for **every** subset of the keys, including `(b)` alone — category totals across all regions — which `rollup` does not produce.

`grouping_id()` tells real NULLs apart from "all" NULLs: it is a bit mask of which keys are rolled up (here region rolled up = binary `10` = 2).'''),

    dict(t='5.13 Quick statistics: describe and summary', tables=['sales'],
         code='''sales.select("qty", "price", "amount").describe().show()
sales.select("amount").summary("count", "min", "25%", "50%", "75%", "max").show()''',
         expl='''`describe()` returns count, mean, stddev, min and max for each column; `summary()` adds approximate percentiles and lets you choose the statistics.

The values come back as **strings** — the output is meant for reading, not for further calculation.'''),

    dict(divider=True, t='Business tables = the Gold layer',
         lead='Aggregations turn clean Silver rows into the tables a dashboard, a report or a model reads directly.'),

    dict(t='5.14 Build a Gold table: monthly revenue per region', tables=['sales'],
         code='''gold = (sales
    .groupBy(F.date_trunc("month", "order_date").cast("date").alias("month"), "region")
    .agg(F.count("*").alias("orders"),
         F.sum("qty").alias("units"),
         F.round(F.sum("amount"), 2).alias("revenue"),
         F.round(F.avg("amount"), 2).alias("avg_order_value")))

gold.write.mode("overwrite").partitionBy("month").parquet("data/gold/region_monthly")

(spark.read.parquet("data/gold/region_monthly")
      .orderBy("month", F.desc("revenue")).show())''',
         expl='''**Gold** tables answer a business question directly — here "how much did each region sell each month?" — at the grain the consumer needs (one row per month × region), with clear names and rounded values.

A dashboard reads 10 rows instead of re-aggregating millions of order lines on every refresh. Partitioning by `month` lets it read only the months it shows — and, as in Chapter 3, the partition column comes back as the **last** column when the table is read.'''),
]

POINTS = '''
1. `df.agg(...)` aggregates the whole table into one row; `df.groupBy(keys).agg(...)` returns one row per group.
2. Always `alias` aggregates. `count("*")` counts rows; `count(col)`, `sum`, `avg`, `min`, `max` **ignore NULLs**.
3. WHERE = `filter` **before** `groupBy`; HAVING = `filter` **after** `agg`.
4. `sum(when(cond, x))` / `count_if(cond)` give several filtered totals in one pass.

    | SQL | PySpark |
    |---|---|
    | `GROUP BY a HAVING SUM(x) > 10` | `.groupBy("a").agg(F.sum("x").alias("s")).filter("s > 10")` |
    | `COUNT(DISTINCT x)` | `F.countDistinct("x")` (exact) / `F.approx_count_distinct("x")` |
    | `SUM(CASE WHEN c THEN x ELSE 0 END)` | `F.sum(F.when(c, F.col("x")).otherwise(0))` |
    | `STRING_AGG` / `ARRAY_AGG` | `F.collect_list`, `F.collect_set` |
    | `GROUP BY ROLLUP(a, b)` | `.rollup("a", "b")` |
    | `GROUP BY CUBE(a, b)` | `.cube("a", "b")` |

<!--break-->
5. `pivot(col, [values])` — list the values to save a job; `unpivot` reverses it.
6. Every `groupBy` is a shuffle; partial aggregation before the shuffle keeps it cheap for sum/count, but not for `collect_list` or exact distinct counts.
7. **Gold** = business-ready aggregates at the grain a report needs, written once and read many times.
'''

EXTRA_TITLE = 'Deep dive: how Spark aggregates'
EXTRA_LEAD = 'Why sum is cheap, countDistinct is expensive, and what the plan shows.'
EXTRA = [
    dict(t='Two-phase aggregation in the plan', tables=['sales'],
         code='''sales.groupBy("region").agg(F.sum("amount")).explain()''',
         expl='''Read it bottom-up: `HashAggregate(partial_sum)` sums **inside each partition** first, the `Exchange` shuffles only one row per region per partition, and the top `HashAggregate` adds the partial sums.

That is why `sum`, `count`, `min`, `max` and `avg` scale well — the shuffle carries partial results, not raw rows (the same idea as a combiner in MapReduce).'''),
    dict(t='countDistinct needs an extra aggregation', tables=['sales'],
         code='''sales.groupBy("region").agg(F.countDistinct("product")).explain()''',
         expl='''An exact distinct count cannot be pre-summed: Spark first groups by `(region, product)` to remove duplicates, then counts per region — more `HashAggregate` steps and a second shuffle.

With high-cardinality columns this is where jobs slow down. Use `approx_count_distinct` when a small error is acceptable.'''),
    dict(t='NULL keys form their own group',
         code='''t = spark.createDataFrame([("A", 1), (None, 2), (None, 3), ("A", 4)], "k STRING, v INT")
t.groupBy("k").agg(F.sum("v").alias("total"), F.count("k").alias("count_k")).show()''',
         expl='''All NULL keys are grouped together (as in SQL). But `count("k")` is **0** for that group, because `count(col)` skips NULLs — use `count("*")` to count the rows.'''),
]

MCQS = [
    dict(q='How do you write `HAVING SUM(amount) > 1000` in PySpark?', kind='concept',
         o=['`.groupBy("r").having(F.sum("amount") > 1000)`', '`.filter(F.sum("amount") > 1000).groupBy("r")`',
            '`.groupBy("r").agg(F.sum("amount").alias("s")).filter("s > 1000")`', '`.groupBy("r").where("amount > 1000")`'], a=2,
         why='There is no having method; filter the aggregated result after agg.'),
    dict(q='What does this print?', kind='output', run=True, check='[1, 1]',
         code='''df = spark.createDataFrame([("a", 1), ("a", None), ("b", 5)], "k STRING, v INT")
r = df.groupBy("k").agg(F.count("v").alias("c")).orderBy("k").collect()
print([x.c for x in r])''',
         o=['`[2, 1]`', '`[1, 1]`', '`[2, 0]`', '`[3]`'], a=1,
         why='`count("v")` skips NULLs: group a has one non-NULL value. (`count("*")` would give 2.)'),
    dict(q='Why can `countDistinct` be much slower than `sum` on big data?', kind='concept',
         o=['It runs in Python', 'It cannot be partially aggregated — every distinct value must be kept and shuffled', 'It always uses one partition', 'It sorts the whole table first'], a=1,
         why='Sums can be pre-aggregated per partition; distinct values must be deduplicated across all partitions, which needs extra aggregation and shuffling.'),
    dict(q='`df.rollup("region", "category")` — what does a row with region = NULL and category = NULL hold?', kind='concept',
         o=['Rows whose region and category are missing', 'The grand total over all rows', 'Nothing — rollup drops NULLs', 'The average of all groups'], a=1,
         why='In rollup output, NULL in a key column means "all values" — both NULL is the grand total. Use `grouping_id()` to tell this apart from real NULL data.'),
    dict(q='What does this print?', kind='output', run=True, check='2.0',
         code='''df = spark.createDataFrame([(1,), (None,), (3,)], "x INT")
print(df.agg(F.avg("x")).first()[0])''',
         o=['1.33', '2.0', 'None', '1.0'], a=1,
         why='avg ignores NULLs: (1 + 3) / 2 = 2.0.'),
    dict(q='Why pass the value list in `pivot("category", ["A", "B"])`?', kind='concept',
         o=['Without it, pivot fails', 'It avoids an extra job that finds the distinct values first, and fixes the column order', 'It makes the result sorted', 'It removes NULL values'], a=1,
         why='Without the list, Spark must first compute the distinct values of the pivot column — an extra pass over the data.'),
    dict(q='The output has a column named `sum(amount)` instead of `revenue`. What is missing?', kind='debug',
         code='''sales.groupBy("region").agg(F.sum("amount"))''',
         o=['`.alias("revenue")` on the aggregate', 'An `orderBy`', '`F.col("revenue")`', 'A `select` before `groupBy`'], a=0,
         why='Unnamed aggregates get generated names. `F.sum("amount").alias("revenue")` names it.'),
    dict(q='Which gives the number of orders with amount ≥ 5000 per region in one pass?', kind='concept',
         o=['`.groupBy("region").count_if("amount >= 5000")`', '`.groupBy("region").agg(F.count_if(F.col("amount") >= 5000))`', '`.filter("amount >= 5000").count()`', '`.agg(F.when(F.count("*") >= 5000))`'], a=1,
         why='`count_if(condition)` is an aggregate that counts rows where the condition is true.'),
    dict(q='What does this print?', kind='output', run=True, check="['x', 'y']",
         code='''df = spark.createDataFrame([("g", "y"), ("g", "x"), ("g", "y")], "k STRING, v STRING")
print(df.groupBy("k").agg(F.array_sort(F.collect_set("v")).alias("s")).first().s)''',
         o=["`['y', 'x', 'y']`", "`['x', 'y']`", "`['y', 'x']`", '`2`'], a=1,
         why='collect_set removes the duplicate y, and array_sort orders the result.'),
]

PRACTICE = dict(
    kinds=['Rewrite GROUP BY / HAVING reports from your SQL notes, including conditional sums.',
           'Monthly and region × category summaries, then the same as a pivot table.',
           'Subtotal reports with rollup, and grouping_id to label subtotal rows.'],
    problems=[
        (1693, 'daily-leads-and-partners', 'Daily Leads and Partners', 'Easy', 'groupBy two keys + two `countDistinct`.'),
        (596, 'classes-more-than-5-students', 'Classes More Than 5 Students', 'Easy', 'HAVING = filter after agg.'),
        (1211, 'queries-quality-and-percentage', 'Queries Quality and Percentage', 'Easy', '`avg` of a ratio and `avg(when(...))`.'),
        (1193, 'monthly-transactions-i', 'Monthly Transactions I', 'Medium', '`date_format` month key + conditional sums.'),
        (1907, 'count-salary-categories', 'Count Salary Categories', 'Medium', 'Conditional counts, then unpivot to rows.'),
    ])
