ORDER = 7
KEY = '7'
TITLE = 'Window Functions: ranking, lag/lead, running totals'
WEEK = 'Week 3'
DATA = ['purchases']

SETUP_TITLE = '7.0 The data — purchases per customer'
SETUP = '''from pyspark.sql import functions as F
from pyspark.sql.window import Window

purchases = spark.createDataFrame([
    (1, "Amit",  "Pune",   "2026-01-03", "Electronics", 1500),
    (2, "Amit",  "Pune",   "2026-01-10", "Fashion",      800),
    (3, "Amit",  "Pune",   "2026-02-02", "Electronics", 2200),
    (4, "Riya",  "Mumbai", "2026-01-05", "Home",         600),
    (5, "Riya",  "Mumbai", "2026-01-25", "Fashion",     1800),
    (6, "Riya",  "Mumbai", "2026-02-11", "Fashion",     1800),
    (7, "Riya",  "Mumbai", "2026-03-01", "Electronics", 3000),
    (8, "Arjun", "Delhi",  "2026-01-15", "Electronics", 2500),
    (9, "Arjun", "Delhi",  "2026-02-20", "Home",         400),
    (10,"Sneha", "Pune",   "2026-02-14", "Fashion",     1200),
], "purchase_id INT, customer STRING, city STRING, purchase_date STRING, "
   "category STRING, amount INT") \\
   .withColumn("purchase_date", F.to_date("purchase_date"))'''
SETUP_EXPL = '''Ten purchases by four customers. A **window function** computes a value for each row from a group of related rows (its *window*) **without collapsing them** — unlike `groupBy`, which returns one row per group.

A window is described with `Window.partitionBy(...)` (which rows belong together), `.orderBy(...)` (their order) and optionally a frame (`rowsBetween` / `rangeBetween`). Then `F.some_function().over(window)` uses it.'''

SECTIONS = [
    dict(t='7.1 Each purchase next to its customer total (aggregate over a window)', tables=['purchases'],
         code='''by_cust = Window.partitionBy("customer")

(purchases.select("customer", "purchase_date", "amount",
                  F.sum("amount").over(by_cust).alias("cust_total"),
                  F.round(F.col("amount") / F.sum("amount").over(by_cust) * 100, 1).alias("pct"))
          .orderBy("customer", "purchase_date").show())''',
         expl='''`F.sum("amount").over(by_cust)` repeats the customer's total on **every** row of that customer — all 10 rows stay. That makes "share of total" a one-line calculation.

With `groupBy("customer")` you would get 4 rows and have to join them back to the 10.'''),

    dict(t="7.2 Number each customer's purchases (row_number)", tables=['purchases'], home=True,
         code='''w = Window.partitionBy("customer").orderBy("purchase_date")

(purchases.withColumn("nth", F.row_number().over(w))
          .select("customer", "purchase_date", "amount", "nth")
          .orderBy("customer", "nth").show())''',
         expl='''`row_number()` numbers rows 1, 2, 3 … inside each partition in the window's order. It restarts at 1 for every customer.

Ranking functions **require** an `orderBy` in the window; without one Spark raises an error.'''),

    dict(t="7.3 Only each customer's first purchase (top-N per group)", tables=['purchases'],
         code='''first_buys = (purchases
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn"))
first_buys.select("customer", "purchase_date", "category", "amount").show()''',
         expl='''Window function, then `filter` on its result — the standard **top-N per group** pattern (`rn <= 3` for the top 3). You cannot filter on a window function in the same `select`; compute it first.

The same pattern **deduplicates** data: order by "latest first" and keep `rn == 1` to keep the newest record per key.'''),

    dict(t='7.4 Ties: row_number vs rank vs dense_rank', tables=['purchases'],
         code='''by_amount = Window.partitionBy("customer").orderBy(F.desc("amount"))

(purchases.filter(F.col("customer") == "Riya")
          .select("purchase_date", "amount",
                  F.row_number().over(by_amount).alias("row_number"),
                  F.rank().over(by_amount).alias("rank"),
                  F.dense_rank().over(by_amount).alias("dense_rank"))
          .show())''',
         expl='''Riya has two purchases of 1800. **`row_number`** breaks the tie arbitrarily (1, 2, 3, 4). **`rank`** gives both 2 and **skips** 3 (1, 2, 2, 4). **`dense_rank`** gives both 2 and does **not** skip (1, 2, 2, 3).

For "second highest amount" use `dense_rank`; for "exactly one row per group" use `row_number` with a tie-breaker column in `orderBy`.'''),

    dict(t='7.5 Compare with the previous purchase (lag)', tables=['purchases'],
         code='''(purchases
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("change", F.col("amount") - F.col("prev_amount"))
    .withColumn("days_since_prev", F.datediff("purchase_date", F.lag("purchase_date").over(w)))
    .select("customer", "purchase_date", "amount", "prev_amount", "change", "days_since_prev")
    .orderBy("customer", "purchase_date").show())''',
         expl='''`lag(col, n=1)` reads the value from n rows **before** the current one in the window order. The first row of each customer has no previous row, so it gets NULL (and so does the difference).

`lag(col, 1, default)` supplies a default instead of NULL.'''),

    dict(t='7.6 Look ahead (lead)', tables=['purchases'],
         code='''(purchases
    .withColumn("next_category", F.lead("category").over(w))
    .select("customer", "purchase_date", "category", "next_category")
    .orderBy("customer", "purchase_date").show())''',
         expl='''`lead` reads **forward**. Here: after buying something in one category, what did the customer buy next? The last purchase of each customer has no next row → NULL.

This is the basis of "next best product" and funnel analysis.'''),

    dict(t='7.7 Running total per customer (rowsBetween)', tables=['purchases'],
         code='''running = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

(purchases.withColumn("running_total", F.sum("amount").over(running))
          .select("customer", "purchase_date", "amount", "running_total")
          .orderBy("customer", "purchase_date").show())''',
         expl='''A **frame** narrows the window to some rows around the current one. `unboundedPreceding → currentRow` = from the first row of the partition up to this row, so the sum grows row by row.

`rowsBetween(-2, 0)` would be "this row and the two before it" — a moving window.'''),

    dict(t='7.8 Moving average of the last two purchases', tables=['purchases'],
         code='''last_two = w.rowsBetween(-1, Window.currentRow)

(purchases.withColumn("avg_last_2", F.avg("amount").over(last_two))
          .select("customer", "purchase_date", "amount", "avg_last_2")
          .orderBy("customer", "purchase_date").show())''',
         expl='''`rowsBetween(-1, 0)` = the previous row and this one. The first row of each customer has no previous row, so its "average of two" is just itself.

Use `rowsBetween` for "the last N **rows**"; use `rangeBetween` for "the last N **days**" (next slide).'''),

    dict(t='7.9 Spend in the last 30 days (rangeBetween on dates)', tables=['purchases'],
         code='''by_day = (Window.partitionBy("customer")
          .orderBy(F.col("purchase_date").cast("timestamp").cast("long"))
          .rangeBetween(-30 * 86400, 0))

(purchases.withColumn("spend_30d", F.sum("amount").over(by_day))
          .select("customer", "purchase_date", "amount", "spend_30d")
          .orderBy("customer", "purchase_date").show())''',
         expl='''`rangeBetween` uses **values** of the ordering column, not row positions. Ordering by the date as seconds since 1970 makes `-30 * 86400` mean "30 days back". Riya's 2026-02-11 purchase counts the 2026-01-25 one (17 days earlier) but not 2026-01-05 (37 days).

A `rowsBetween` frame would give a wrong answer whenever purchases are not evenly spaced.'''),

    dict(t='7.10 The last_value trap — and the frame that fixes it', tables=['purchases'],
         code='''whole = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

(purchases.filter("customer = 'Amit'")
    .select("purchase_date", "category",
            F.first("category").over(w).alias("first_cat"),
            F.last("category").over(w).alias("last_default"),
            F.last("category").over(whole).alias("last_whole"))
    .show())''',
         expl='''When a window has `orderBy`, the **default frame** ends at the **current row** (`unboundedPreceding → currentRow`). So `last` returns the current row's own value — `last_default` just copies `category`.

To see the true last value of the partition, widen the frame to `unboundedFollowing`. `first` is safe either way because the frame always starts at the first row.'''),

    dict(t='7.11 Where does each purchase stand? percent_rank and ntile', tables=['purchases'],
         code='''overall = Window.orderBy("amount")

(purchases.select("purchase_id", "amount",
                  F.round(F.percent_rank().over(overall), 2).alias("pct_rank"),
                  F.ntile(3).over(overall).alias("tier"))
          .orderBy("amount").show())''',
         expl='''`percent_rank` scales the rank to 0 … 1; `ntile(3)` splits the rows into 3 nearly equal buckets — low, mid and high spenders.

This window has **no `partitionBy`**: every row goes to a single partition. Fine for 10 rows; on real data it moves the whole table onto one executor, and Spark logs a warning.'''),

    dict(t='7.12 Running totals across the whole table, by month', tables=['purchases'],
         code='''monthly = (purchases
    .groupBy(F.date_trunc("month", "purchase_date").cast("date").alias("month"))
    .agg(F.sum("amount").alias("revenue")))

(monthly.withColumn("cumulative", F.sum("revenue").over(Window.orderBy("month")))
        .withColumn("mom_change", F.col("revenue") - F.lag("revenue").over(Window.orderBy("month")))
        .orderBy("month").show())''',
         expl='''Windows combine with aggregation: first `groupBy` month, then window over the small monthly result — cumulative revenue and month-over-month change.

Aggregating first means the un-partitioned window only sees 3 rows, so the single-partition warning does not matter here.'''),
]

POINTS = '''
1. `Window.partitionBy(...).orderBy(...)` defines the window; `F.fn().over(window)` applies it. Rows are **not** collapsed.
2. Ranking (`row_number`, `rank`, `dense_rank`, `percent_rank`, `ntile`) needs an `orderBy`.

    | Values 300, 200, 200, 100 | row_number | rank | dense_rank |
    |---|---|---|---|
    | 300 | 1 | 1 | 1 |
    | 200 | 2 | 2 | 2 |
    | 200 | 3 | 2 | 2 |
    | 100 | 4 | 4 | 3 |

3. Top-N per group / dedup: add `row_number()` then `filter(rn <= N)` — you cannot filter a window function in the same `select`.
4. `lag` looks back, `lead` looks forward; edges get NULL unless you give a default.
5. Frames: `rowsBetween(start, end)` counts **rows**, `rangeBetween` uses **values** (e.g. seconds for "last 30 days").
6. With `orderBy` the default frame ends at the **current row** → `last()` returns the current row. Use `unboundedFollowing` for the partition's real last value.
7. A window without `partitionBy` puts all data in **one partition** — aggregate first or add a partition key.
8. Every window with `partitionBy` costs a shuffle + sort by (partition keys, order keys).
'''

EXTRA_TITLE = 'Deep dive: windows in practice'
EXTRA_LEAD = 'Two classic interview patterns and what a window costs.'
EXTRA = [
    dict(t='Gaps and islands: streaks of consecutive days',
         code='''logins = spark.createDataFrame(
    [("u1", d) for d in ["2026-03-01", "2026-03-02", "2026-03-03", "2026-03-06", "2026-03-07"]]
    + [("u2", d) for d in ["2026-03-01", "2026-03-03"]],
    "user STRING, day STRING").withColumn("day", F.to_date("day"))

uw = Window.partitionBy("user").orderBy("day")
streaks = (logins
    .withColumn("grp", F.date_sub("day", F.row_number().over(uw)))
    .groupBy("user", "grp")
    .agg(F.min("day").alias("start"), F.max("day").alias("end"), F.count("*").alias("days")))
streaks.orderBy("user", "start").show()''',
         expl='''Subtract the row number from the date: for consecutive days both go up by 1, so the difference (`grp`) stays constant inside a streak and jumps when there is a gap. Group by it to get each streak's start, end and length.

u1 has a 3-day streak and a 2-day streak; u2 never logged in two days in a row.'''),
    dict(t='Sessionization: a new session after 30 minutes of silence',
         code='''clicks = spark.createDataFrame([
    ("u1", "2026-03-01 10:00:00"), ("u1", "2026-03-01 10:10:00"), ("u1", "2026-03-01 11:00:00"),
    ("u1", "2026-03-01 11:05:00"), ("u2", "2026-03-01 09:00:00")], "user STRING, ts STRING") \\
    .withColumn("ts", F.to_timestamp("ts"))

cw = Window.partitionBy("user").orderBy("ts")
gap_min = (F.col("ts").cast("long") - F.lag("ts").over(cw).cast("long")) / 60

sessions = (clicks
    .withColumn("new_session", F.when(gap_min.isNull() | (gap_min > 30), 1).otherwise(0))
    .withColumn("session_id", F.sum("new_session").over(cw.rowsBetween(Window.unboundedPreceding, 0))))
sessions.show()''',
         expl='''Two windows: `lag` measures the gap to the previous click, and a **running sum** of the "new session" flags numbers the sessions. Every gap over 30 minutes starts session 2, 3, …

This is how web analytics builds sessions from raw click logs.'''),
    dict(t='What a window costs in the plan', tables=['purchases'],
         code='''purchases.withColumn("rn", F.row_number().over(w)).explain()''',
         expl='''`Exchange hashpartitioning(customer)` shuffles each customer's rows together, `Sort [customer, purchase_date]` orders them, and `Window` computes the function in one pass over each sorted partition.

Several window functions that share the **same** partitionBy/orderBy run in a single Window step — define the `Window` once and reuse it.'''),
]

MCQS = [
    dict(q='What does this print?', kind='output', run=True, check='[1, 2, 2, 4]',
         code='''df = spark.createDataFrame([(300,), (200,), (200,), (100,)], "v INT")
w = Window.orderBy(F.desc("v"))
print([r[0] for r in df.select(F.rank().over(w)).collect()])''',
         o=['`[1, 2, 3, 4]`', '`[1, 2, 2, 3]`', '`[1, 2, 2, 4]`', '`[1, 1, 2, 3]`'], a=2,
         why='rank gives ties the same number and skips the next one: 1, 2, 2, 4.'),
    dict(q='Which function gives the second-highest **distinct** salary per department, even with ties at the top?', kind='concept',
         o=['`row_number`', '`rank`', '`dense_rank`', '`ntile(2)`'], a=2,
         why='dense_rank does not skip numbers, so dense_rank == 2 is always the second distinct value.'),
    dict(q='Why does `F.last("x").over(Window.partitionBy("k").orderBy("t"))` return the current row\'s value?', kind='concept',
         o=['last() is broken in Spark', 'The default frame with orderBy ends at the current row', 'orderBy sorts descending', 'last() ignores partitionBy'], a=1,
         why='With orderBy, the default frame is unboundedPreceding → currentRow. Widen it to unboundedFollowing.'),
    dict(q='What does this print?', kind='output', run=True, check='[None, 10, 20]',
         code='''df = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], "t INT, v INT")
print([r[0] for r in df.select(F.lag("v").over(Window.orderBy("t"))).collect()])''',
         o=['`[10, 20, 30]`', '`[None, 10, 20]`', '`[20, 30, None]`', '`[0, 10, 20]`'], a=1,
         why='lag reads the previous row; the first row has none, so it is NULL (None in Python).'),
    dict(q='This code fails. Why?', kind='debug',
         code='''df.filter(F.row_number().over(w) == 1)''',
         o=['row_number needs an argument', 'Window functions are not allowed in a filter — compute the column first, then filter it', 'filter only accepts strings', 'w must not have orderBy'], a=1,
         why='Window functions can only appear in select/withColumn. Add the column with withColumn, then filter on it.'),
    dict(q='Which frame gives "spend in the last 7 days" correctly when purchase dates are irregular?', kind='concept',
         o=['`rowsBetween(-6, 0)`', '`rangeBetween(-7 * 86400, 0)` ordered by the timestamp in seconds', '`rowsBetween(-7, 0)`', 'No frame — the default works'], a=1,
         why='rangeBetween uses the values of the order column, so it covers 7 days whatever the number of rows.'),
    dict(q='What is the risk of `Window.orderBy("amount")` with no partitionBy on a billion rows?', kind='concept',
         o=['None', 'All rows are moved to a single partition and sorted by one task', 'It runs in Python', 'It drops NULL rows'], a=1,
         why='Without partition keys, Spark must place the whole dataset into one partition to order it.'),
    dict(q='What does this print?', kind='output', run=True, check='[5, 15, 30]',
         code='''df = spark.createDataFrame([(1, 5), (2, 10), (3, 15)], "t INT, v INT")
rw = Window.orderBy("t").rowsBetween(Window.unboundedPreceding, 0)
print([r[0] for r in df.select(F.sum("v").over(rw)).collect()])''',
         o=['`[30, 30, 30]`', '`[5, 15, 30]`', '`[5, 10, 15]`', '`[15, 25, 30]`'], a=1,
         why='A frame from the first row to the current row gives a running total: 5, 5+10, 5+10+15.'),
]

PRACTICE = dict(
    kinds=['Top-N per group and "keep the latest record" dedup with row_number.',
           'Rank vs dense_rank questions ("Nth highest").',
           'lag/lead comparisons, running totals, moving averages, gaps and islands.'],
    problems=[
        (178, 'rank-scores', 'Rank Scores', 'Medium', '`dense_rank` over a descending order.'),
        (176, 'second-highest-salary', 'Second Highest Salary', 'Medium', '`dense_rank` = 2, and NULL when it does not exist.'),
        (184, 'department-highest-salary', 'Department Highest Salary', 'Medium', '`rank` partitioned by department, keep 1.'),
        (185, 'department-top-three-salaries', 'Department Top Three Salaries', 'Hard', '`dense_rank <= 3` per department.'),
        (180, 'consecutive-numbers', 'Consecutive Numbers', 'Medium', '`lag` twice, or gaps and islands.'),
        (1321, 'restaurant-growth', 'Restaurant Growth', 'Medium', '7-day moving sum with `rangeBetween`.'),
        (1204, 'last-person-to-fit-in-the-bus', 'Last Person to Fit in the Bus', 'Medium', 'Running total, filter, take the last.'),
    ])
