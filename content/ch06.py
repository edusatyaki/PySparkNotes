ORDER = 6
KEY = '6'
TITLE = 'Joins: inner, outer, semi, anti, self and broadcast'
WEEK = 'Week 3'
DATA = ['customers', 'orders', 'products', 'staff']

SETUP_TITLE = '6.0 The data — customers, orders and products'
SETUP = '''from pyspark.sql import functions as F

customers = spark.createDataFrame([
    (1, "Amit", "Pune"), (2, "Riya", "Mumbai"), (3, "Arjun", "Delhi"),
    (4, "Sneha", "Pune"), (5, "Karan", None),
], "cust_id INT, name STRING, city STRING")

orders = spark.createDataFrame([
    (101, 1, "P1", 2), (102, 1, "P3", 1), (103, 2, "P2", 1),
    (104, 3, "P1", 1), (105, 9, "P2", 3), (106, None, "P4", 1),
], "order_id INT, cust_id INT, product_id STRING, qty INT")

products = spark.createDataFrame([
    ("P1", "Headphones", 1500.0), ("P2", "Phone", 18000.0),
    ("P3", "Charger", 600.0), ("P5", "Tablet", 25000.0),
], "product_id STRING, product STRING, price DOUBLE")

staff = spark.createDataFrame([
    (1, "Meera", None), (2, "Rahul", 1), (3, "Isha", 1), (4, "Dev", 2),
], "emp_id INT, name STRING, manager_id INT")'''
SETUP_EXPL = '''The data is built to show every join case:

- Customers 4 (Sneha) and 5 (Karan) have **no orders**.
- Order 105 belongs to customer **9, who does not exist**; order 106 has a **NULL** customer.
- Product P4 is ordered but **not in the catalogue**; P5 is in the catalogue but **never ordered**.
- `staff` points to itself through `manager_id`, for the self join.'''

SECTIONS = [
    dict(t='6.1 Orders with their customer (inner join)', tables=['orders', 'customers'], home=True,
         code='''orders.join(customers, on="cust_id", how="inner") \\
      .select("order_id", "cust_id", "name", "qty") \\
      .orderBy("order_id").show()''',
         expl='''`join(other, on, how)` — with `on` given as a column **name** (or a list of names), Spark keeps a single `cust_id` column in the result.

An **inner** join keeps only matching pairs. Order 105 (customer 9) and order 106 (NULL customer) are gone, and so are customers 4 and 5. Amit appears twice because he has two orders.'''),

    dict(t='6.2 When the key names differ: a join condition', tables=['orders', 'products'],
         code='''o = orders.alias("o")
p = products.alias("p")

(o.join(p, F.col("o.product_id") == F.col("p.product_id"), "inner")
  .select("o.order_id", "p.product", (F.col("o.qty") * F.col("p.price")).alias("amount"))
  .orderBy("order_id").show())''',
         expl='''With an expression as `on`, **both** key columns are kept — so give each side an `alias` and qualify columns as `"o.order_id"`. Without aliases, `select("product_id")` would be ambiguous.

Any boolean expression works as a join condition, including ranges and several `&`-joined comparisons.'''),

    dict(t='6.3 Every customer, even without orders (left join)', tables=['customers', 'orders'],
         code='''customers.join(orders, "cust_id", "left") \\
         .select("cust_id", "name", "order_id") \\
         .orderBy("cust_id", "order_id").show()''',
         expl='''A **left** (outer) join keeps every row of the left table; where no order matches, the order columns are NULL — Sneha and Karan.

`how` accepts `"left"`, `"left_outer"` or `"leftouter"` — all the same.'''),

    dict(t='6.4 Customers who never ordered (left_anti)', tables=['customers', 'orders'],
         code='''customers.join(orders, "cust_id", "left_anti").show()''',
         expl='''A **left anti** join returns left rows that have **no** match — SQL `NOT EXISTS`. Only the left table's columns come back.

It is cleaner and faster than "left join, then filter `order_id IS NULL`", and unlike `NOT IN` it is not broken by the NULL `cust_id` in `orders`.'''),

    dict(t='6.5 Customers who ordered at least once (left_semi)', tables=['customers', 'orders'],
         code='''customers.join(orders, "cust_id", "left_semi").orderBy("cust_id").show()''',
         expl='''A **left semi** join returns left rows that have **at least one** match — SQL `EXISTS` / `IN`. Amit appears **once** even though he has two orders: a semi join never duplicates left rows, which an inner join + `distinct` would need to undo.'''),

    dict(t='6.6 All orders, matched or not (right join)', tables=['customers', 'orders'],
         code='''customers.join(orders, "cust_id", "right") \\
         .select("order_id", "cust_id", "name").orderBy("order_id").show()''',
         expl='''A **right** join keeps every row of the right table (`orders`). Orders 105 (customer 9) and 106 (NULL) get a NULL `name`.

Most teams write it as a left join with the tables swapped, so every query reads "main table first".'''),

    dict(t='6.7 Everything from both sides (full outer join)', tables=['customers', 'orders'],
         code='''customers.join(orders, "cust_id", "full") \\
         .select("cust_id", "name", "order_id") \\
         .orderBy(F.col("cust_id").asc_nulls_last(), "order_id").show()''',
         expl='''A **full** outer join keeps unmatched rows from both sides: customers without orders and orders without a known customer. It is the standard tool for **reconciling** two sources ("what is in A but not B, and B but not A").'''),

    dict(t='6.8 Three tables at once', tables=['orders', 'customers', 'products'],
         code='''report = (orders
    .join(customers, "cust_id", "left")
    .join(products, "product_id", "left")
    .select("order_id", "name", "product", "qty",
            (F.col("qty") * F.col("price")).alias("amount"))
    .orderBy("order_id"))
report.show()''',
         expl='''Joins chain like any transformation. Starting from `orders` with left joins keeps all 6 orders; missing customers and the uncatalogued product P4 show up as NULLs instead of vanishing — that is usually what a report needs.

Spark may reorder inner joins for efficiency; it never reorders outer joins in a way that changes the result.'''),

    dict(t='6.9 Who reports to whom? (self join)', tables=['staff'],
         code='''e = staff.alias("e")
m = staff.alias("m")

(e.join(m, F.col("e.manager_id") == F.col("m.emp_id"), "left")
  .select(F.col("e.name").alias("employee"), F.col("m.name").alias("manager"))
  .show())''',
         expl='''A self join is a join of a table with itself, and the aliases `e` and `m` are what make it possible — without them every column name is ambiguous.

Meera has no manager (`manager_id` NULL), so the left join keeps her with a NULL manager.'''),

    dict(t='6.10 Every possible pairing (crossJoin)', tables=['customers', 'products'],
         code='''pairs = customers.select("name").crossJoin(products.select("product"))
print(pairs.count())
pairs.show(6)''',
         expl='''`crossJoin` pairs every row with every row: 5 × 4 = 20. Use it deliberately — for a small dimension such as "every store × every date" — because on two large tables it explodes.

Spark makes you call `crossJoin` (or confirm a cartesian product) explicitly so you do not create one by accident.'''),

    dict(t='6.11 NULL keys never match — unless you ask (eqNullSafe)',
         code='''a = spark.createDataFrame([(1, "x"), (None, "y")], "k INT, va STRING")
b = spark.createDataFrame([(1, "p"), (None, "q")], "k INT, vb STRING")

a.join(b, a.k == b.k, "inner").show()
a.join(b, a.k.eqNullSafe(b.k), "inner").show()''',
         expl='''With `==`, a NULL key never equals anything, not even another NULL, so only `k = 1` matches. `eqNullSafe` (SQL `<=>`) treats NULL as equal to NULL.

Here the condition uses `a.k` and `b.k`: the `df.col` style is how you tell two same-named columns apart without aliases.'''),

    dict(t='6.12 Stack rows from two tables: union and unionByName',
         code='''online = spark.createDataFrame([(1, "Pune", "web")], "id INT, city STRING, channel STRING")
store  = spark.createDataFrame([(2, "store", "Delhi")], "id INT, channel STRING, city STRING")

online.union(store).show()          # by position: wrong!
online.unionByName(store).show()    # by name: right''',
         expl='''`union` matches columns by **position**, like SQL `UNION ALL`. The store table lists `channel` before `city`, so its row lands with `city = store` and `channel = Delhi`. Both columns are strings, so nothing fails — the data is just wrong.

Use `unionByName`. With `allowMissingColumns=True` it also fills columns missing on one side with NULL. Neither removes duplicates; add `distinct()` for SQL `UNION`.'''),
]

POINTS = '''
1. `left.join(right, on, how)` — `on` as a name or list of names keeps one key column; an expression keeps both, so alias the DataFrames.
2. Join types:

    | how | Keeps | SQL |
    |---|---|---|
    | `inner` | matching pairs only | `INNER JOIN` |
    | `left` / `right` | all of one side + matches | `LEFT / RIGHT JOIN` |
    | `full` | everything from both sides | `FULL OUTER JOIN` |
    | `left_semi` | left rows **with** a match (no duplicates, left columns only) | `EXISTS` / `IN` |
    | `left_anti` | left rows **without** a match | `NOT EXISTS` |
    | `crossJoin` | every pairing | `CROSS JOIN` |

3. One-to-many joins repeat the "one" side; check row counts before and after a join.
4. NULL keys never match with `==`; use `eqNullSafe` if NULL should equal NULL.
5. Self joins need `alias` on both copies.
6. `union` is by position, `unionByName` by name — prefer `unionByName`.
7. Joins are wide transformations: both sides shuffle on the key unless one side is small enough to **broadcast**.
'''

EXTRA_TITLE = 'Deep dive: how Spark executes a join'
EXTRA_LEAD = 'Broadcast hash join vs sort-merge join, and how to read which one you got.'
EXTRA = [
    dict(t='In-memory tables: Spark cannot measure them, so it sort-merges', tables=['orders', 'customers'],
         code='''orders.join(customers, "cust_id").explain()''',
         expl='''Read bottom-up: **both** sides are shuffled on `cust_id` (two `Exchange hashpartitioning` lines), each partition is sorted, and the sorted streams are merged — a **`SortMergeJoin`**. It works at any size but costs two shuffles and two sorts.

These DataFrames were built with `createDataFrame`, so Spark has no size statistics for them and assumes they are large. Tiny as they are, it does not dare to broadcast.'''),
    dict(t='Tables read from files: Spark knows the size and broadcasts',
         code='''customers.write.mode("overwrite").parquet("data/customers")
orders.write.mode("overwrite").parquet("data/orders")

c_file = spark.read.parquet("data/customers")
o_file = spark.read.parquet("data/orders")
o_file.join(c_file, "cust_id").explain()''',
         expl='''Read from Parquet, Spark knows each table's size on disk. `customers` is far below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default), so the plan becomes a **`BroadcastHashJoin`**: the small side is sent whole to every executor (`BroadcastExchange`) and `orders` is **not shuffled at all**.

That is the fastest join Spark has. Note also `PushedFilters: [IsNotNull(cust_id)]` — the NULL-key filter is pushed into the Parquet reader.'''),
    dict(t='Force it with a broadcast hint', tables=['orders', 'products'],
         code='''orders.join(F.broadcast(products), "product_id").explain()''',
         expl='''`F.broadcast(df)` tells Spark to broadcast that side whatever its estimated size — exactly the fix for the in-memory case two slides back, or for any table whose size Spark cannot estimate after complex transformations.

Never broadcast a table that does not fit comfortably in executor memory: every executor gets a full copy.'''),
    dict(t='The ambiguous column error', error=True,
         code='''joined = orders.join(customers, orders.cust_id == customers.cust_id)
joined.select("cust_id").show()''',
         expl='''With an expression condition both `cust_id` columns survive, and a bare `"cust_id"` cannot pick one: `AMBIGUOUS_REFERENCE`. Fix it by joining on the name (`on="cust_id"`), by selecting `orders.cust_id`, or by aliasing the DataFrames.'''),
]

MCQS = [
    dict(q='Which join returns customers who have **never** placed an order?', kind='concept',
         o=['`customers.join(orders, "cust_id", "left_semi")`', '`customers.join(orders, "cust_id", "left_anti")`', '`customers.join(orders, "cust_id", "inner")`', '`customers.join(orders, "cust_id", "right")`'], a=1,
         why='left_anti keeps left rows with no match on the right — NOT EXISTS.'),
    dict(q='What does this print?', kind='output', run=True, check='3',
         code='''a = spark.createDataFrame([(1,), (1,), (2,)], "k INT")
b = spark.createDataFrame([(1,), (2,), (2,)], "k INT")
print(a.join(b, "k", "left_semi").count())''',
         o=['2', '3', '4', '6'], a=1,
         why='left_semi returns each left row that has at least one match, without duplicating it: all 3 left rows match.'),
    dict(q='What does this print?', kind='output', run=True, check='4',
         code='''a = spark.createDataFrame([(1,), (1,), (2,)], "k INT")
b = spark.createDataFrame([(1,), (2,), (2,)], "k INT")
print(a.join(b, "k", "inner").count())''',
         o=['3', '4', '5', '6'], a=1,
         why='Inner join pairs every match: key 1 → 2 × 1 = 2 rows, key 2 → 1 × 2 = 2 rows, total 4.'),
    dict(q='Two tables both have a NULL in the join key. How many rows does `a.join(b, a.k == b.k)` produce for those NULLs?', kind='concept',
         o=['One', 'Zero', 'Two', 'It raises an error'], a=1,
         why='NULL = NULL is unknown, so NULL keys never match with ==. Use eqNullSafe to match them.'),
    dict(q='What does the plan operator `BroadcastHashJoin` tell you?', kind='concept',
         o=['Both sides were shuffled and sorted', 'The smaller side was copied to every executor, so the large side was not shuffled', 'The join ran in Python', 'The join was cancelled'], a=1,
         why='A broadcast hash join ships the small table to all executors and avoids shuffling the large one.'),
    dict(q='Why can `df1.union(df2)` silently produce wrong data?', kind='concept',
         o=['It removes duplicates', 'It matches columns by position, not by name', 'It sorts the result', 'It only keeps common columns'], a=1,
         why='union aligns columns positionally. If the column order differs, values land in the wrong columns. Use unionByName.'),
    dict(q='The `select` fails with an ambiguous reference error. Which change fixes it?', kind='debug',
         code='''orders.join(customers, orders.cust_id == customers.cust_id) \\
      .select("cust_id", "name")''',
         o=['Use `how="inner"`', 'Join with `on="cust_id"` so only one `cust_id` column is kept', 'Use `crossJoin`', 'Add `.distinct()`'], a=1,
         why='An expression condition keeps both key columns. Joining on the column name keeps one and removes the ambiguity.'),
    dict(q='What is the setting that controls automatic broadcast joins?', kind='concept',
         o=['`spark.sql.shuffle.partitions`', '`spark.sql.autoBroadcastJoinThreshold`', '`spark.executor.memory`', '`spark.sql.adaptive.enabled`'], a=1,
         why='Tables estimated below autoBroadcastJoinThreshold (10 MB by default) are broadcast automatically.'),
    dict(q='What does this print?', kind='output', run=True, check='20',
         code='''x = spark.range(4)
y = spark.range(5)
print(x.crossJoin(y).count())''',
         o=['9', '20', '5', '4'], a=1,
         why='A cross join pairs every row with every row: 4 × 5 = 20.'),
]

PRACTICE = dict(
    kinds=['Every join type on the same pair of tables — predict the row count before running it.',
           'Anti joins for "never" questions, semi joins for "at least once" questions.',
           'Self joins on manager/employee tables; three-table reports with left joins.'],
    problems=[
        (175, 'combine-two-tables', 'Combine Two Tables', 'Easy', 'Left join; missing addresses become NULL.'),
        (183, 'customers-who-never-order', 'Customers Who Never Order', 'Easy', '`left_anti` join.'),
        (1378, 'replace-employee-id-with-the-unique-identifier', 'Replace Employee ID With The Unique Identifier', 'Easy', 'Left join on a key with different meaning on each side.'),
        (1581, 'customer-who-visited-but-did-not-make-any-transactions', 'Customer Who Visited but Did Not Make Any Transactions', 'Easy', 'Anti join, then groupBy count.'),
        (181, 'employees-earning-more-than-their-managers', 'Employees Earning More Than Their Managers', 'Easy', 'Self join with aliases.'),
        (1280, 'students-and-examinations', 'Students and Examinations', 'Easy', 'crossJoin students × subjects, then left join the counts.'),
        (570, 'managers-with-at-least-5-direct-reports', 'Managers with at Least 5 Direct Reports', 'Medium', 'Group the reports, then semi join back.'),
    ])
