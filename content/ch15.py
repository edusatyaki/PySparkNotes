ORDER = 15
KEY = '15'
TITLE = 'Capstone: the ShopKart pipeline, Bronze → Silver → Gold'
DELTA = True     # run on a Spark session with Delta Lake
WEEK = 'Week 8'
DATA = ['landing/2026-03-01', 'landing/2026-03-02', 'bronze_*', 'silver_*', 'gold_*']

SETUP_TITLE = '15.0 ShopKart\'s raw files for two days'
SETUP = '''import json, os, random, shutil
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE = "data/shopkart"
shutil.rmtree(BASE, ignore_errors=True)
rng = random.Random(11)

def write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        f.write(text)

# product catalogue (one file)
write(f"{BASE}/landing/products/products.csv", "product_id,product,category,price\\n" + "\\n".join([
    "P01,Phone,Electronics,18000", "P02,Earbuds,Electronics,1500", "P03,Kurta,Fashion,900",
    "P04,Sneakers,Fashion,2500", "P05,Mixer,Home,3200", "P06,Bedsheet,Home,1100"]))

day1_customers = [("C01", "amit shah", "pune"), ("C02", "Riya Iyer", "Mumbai "), ("C03", "ARJUN rao", "DELHI"),
                  ("C04", "Sneha Patil", "Pune"), ("C05", "Karan Gill", "Bengaluru"), ("C06", "meera nair", "chennai")]
day2_customers = [("C03", "Arjun Rao", "Bengaluru"),          # moved city → SCD2
                  ("C07", "Dev Joshi", "Jaipur")]               # new customer

def orders_csv(day, first_id, n, cust_ids, extra_lines):
    lines = ["order_id,order_ts,customer_id,product_id,qty,status"]
    for oid in range(first_id, first_id + n):
        lines.append(f"{oid},{day} {rng.randint(9, 21):02d}:{rng.randint(0, 59):02d}:00,"
                     f"{rng.choice(cust_ids)},P0{rng.randint(1, 6)},{rng.randint(1, 3)},"
                     f"{rng.choice(['delivered', 'Delivered ', 'SHIPPED', 'shipped'])}")
    return "\\n".join(lines + extra_lines)

def write_day(day, custs, first, n, extra):
    write(f"{BASE}/landing/{day}/customers.json",
          "\\n".join(json.dumps({"customer_id": c, "name": nm, "city": ct, "signup_date": "2026-01-15"})
                    for c, nm, ct in custs))
    write(f"{BASE}/landing/{day}/orders.csv", orders_csv(day, first, n, [c[0] for c in custs], extra))

write_day("2026-03-01", day1_customers, 1001, 24, [
    "1005,2026-03-01 10:15:00,C02,P02,1,delivered",           # duplicate of a real order id
    "1025,2026-03-01 12:00:00,C01,P03,two,delivered",          # qty is not a number
    "1026,2026-03-01 13:30:00,C99,P01,1,shipped"])             # unknown customer

# the correction repeats order 1003 exactly, with a later time and status "cancelled"
o1003 = [l for l in open(f"{BASE}/landing/2026-03-01/orders.csv") if l.startswith("1003,")][0].strip().split(",")
correction = ",".join([o1003[0], "2026-03-02 09:00:00", o1003[2], o1003[3], o1003[4], "cancelled"])
write_day("2026-03-02", day1_customers[:2] + day2_customers, 1027, 14, [correction])

print(sorted(os.path.relpath(os.path.join(r, f), BASE) for r, _, fs in os.walk(BASE) for f in fs))'''
SETUP_EXPL = '''**ShopKart** is a small online shop. Every day its systems drop files into a landing folder: the day's **orders** (CSV) and new or changed **customers** (JSON), plus a one-off **product catalogue**. The generator has a fixed seed, so every run produces the same files.

The files contain the problems a real pipeline must handle: messy names and cities, inconsistent status text, a duplicated order, a non-numeric quantity, an order for a customer who does not exist, a customer who **moves city** on day 2, and a **correction** that cancels a day-1 order.

The goal: Bronze tables that keep everything, Silver tables you can trust, and Gold tables a dashboard can read — re-runnable day after day.'''

SECTIONS = [
    dict(t='15.1 Bronze: land each file exactly as it arrived',
         code='''def land(day):
    stamp = F.lit(day).cast("date")
    orders = (spark.read.option("header", True).csv(f"{BASE}/landing/{day}/orders.csv")
              .select("*", F.col("_metadata.file_name").alias("_source_file"), stamp.alias("_ingest_date")))
    customers = (spark.read.schema("customer_id STRING, name STRING, city STRING, signup_date STRING")
                 .json(f"{BASE}/landing/{day}/customers.json")
                 .select("*", F.col("_metadata.file_name").alias("_source_file"), stamp.alias("_ingest_date")))
    orders.write.format("delta").mode("append").save(f"{BASE}/bronze/orders")
    customers.write.format("delta").mode("append").save(f"{BASE}/bronze/customers")

def land_products():
    (spark.read.option("header", True).csv(f"{BASE}/landing/products/products.csv")
          .write.format("delta").mode("overwrite").save(f"{BASE}/bronze/products"))

land_products()
land("2026-03-01")
for t in ["orders", "customers", "products"]:
    print(f"bronze_{t:10}", spark.read.format("delta").load(f"{BASE}/bronze/{t}").count(), "rows")
spark.read.format("delta").load(f"{BASE}/bronze/orders").filter("order_id IN (1005, 1025, 1026)").show(truncate=False)''',
         expl='''Bronze keeps **every** line — 27 order lines, including both versions of order 1005, the `two` quantity and the unknown customer `C99` — with every value as text, plus `_source_file` and `_ingest_date`. Orders and customers are **appended** day by day; the small product catalogue is simply overwritten.

Nothing is judged here. If tomorrow we discover a better way to parse a field, Bronze still has the original.'''),

    dict(t='15.2 Silver customers: clean, then keep history (SCD2)',
         code='''def silver_customers(day):
    path = f"{BASE}/silver/customers"
    eff = F.lit(day).cast("date")
    clean = (spark.read.format("delta").load(f"{BASE}/bronze/customers")
             .filter(F.col("_ingest_date") == day)
             .select(F.upper(F.trim("customer_id")).alias("customer_id"),
                     F.initcap(F.trim("name")).alias("name"),
                     F.initcap(F.trim("city")).alias("city"),
                     F.to_date("signup_date").alias("signup_date"))
             .dropDuplicates(["customer_id"]))
    if not DeltaTable.isDeltaTable(spark, path):           # first load: everyone is current
        (clean.withColumn("start_date", F.col("signup_date"))
              .withColumn("end_date", F.lit(None).cast("date"))
              .withColumn("is_current", F.lit(True))
              .write.format("delta").save(path))
        return
    dim = DeltaTable.forPath(spark, path)
    current = dim.toDF().filter("is_current")
    changed = (clean.alias("u").join(current.alias("c"), "customer_id")
               .filter("u.city <> c.city OR u.name <> c.name").select("u.*"))
    staged = (changed.withColumn("merge_key", F.lit(None).cast("string"))
              .unionByName(clean.withColumn("merge_key", F.col("customer_id"))))
    (dim.alias("t").merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current")
        .whenMatchedUpdate(condition="t.city <> s.city OR t.name <> s.name",
                           set={"is_current": "false", "end_date": F.date_sub(eff, 1)})
        .whenNotMatchedInsert(values={"customer_id": "s.customer_id", "name": "s.name", "city": "s.city",
                                      "signup_date": "s.signup_date", "start_date": eff,
                                      "end_date": "null", "is_current": "true"})
        .execute())

silver_customers("2026-03-01")
spark.read.format("delta").load(f"{BASE}/silver/customers").orderBy("customer_id").show()''',
         expl='''Cleaning: trimmed IDs, proper-case names and cities ("DELHI" → "Delhi", "Mumbai " → "Mumbai"), real dates, one row per customer.

History: the first load makes everyone current from their signup date. Later loads use the **SCD2 MERGE** from Chapter 12 — a changed customer's current row is closed the day before the change, and the new version starts on the load date. Day 2 will exercise it.'''),

    dict(t='15.3 Silver products: a small dimension, simply overwritten',
         code='''def silver_products():
    (spark.read.format("delta").load(f"{BASE}/bronze/products")
          .select(F.upper(F.trim("product_id")).alias("product_id"), "product", "category",
                  F.col("price").cast("double").alias("price"))
          .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
          .save(f"{BASE}/silver/products"))

silver_products()
spark.read.format("delta").load(f"{BASE}/silver/products").orderBy("product_id").show()''',
         expl='''Not every table needs MERGE or history. A six-row catalogue is cheapest to rebuild completely on each run. (If prices change and old orders must keep the price at the time of sale, the product dimension would become SCD2 as well — a good extension exercise.)'''),

    dict(t='15.4 Silver orders: validate, quarantine, deduplicate, MERGE',
         code='''def silver_orders(day):
    typed = (spark.read.format("delta").load(f"{BASE}/bronze/orders")
             .filter(F.col("_ingest_date") == day)
             .select(F.col("order_id").cast("int").alias("order_id"),
                     F.to_timestamp("order_ts").alias("order_ts"),
                     F.upper(F.trim("customer_id")).alias("customer_id"),
                     F.upper(F.trim("product_id")).alias("product_id"),
                     F.col("qty").try_cast("int").alias("qty"),
                     F.lower(F.trim("status")).alias("status")))
    known = (spark.read.format("delta").load(f"{BASE}/silver/customers")
             .select("customer_id").distinct().withColumn("known", F.lit(True)))
    checked = (typed.join(known, "customer_id", "left")
               .withColumn("reason", F.when(F.col("qty").isNull(), "qty is not a number")
                                      .when(F.col("known").isNull(), "unknown customer")))
    (checked.filter("reason IS NOT NULL").drop("known")
            .withColumn("_ingest_date", F.lit(day).cast("date"))
            .write.format("delta").mode("append").save(f"{BASE}/silver/orders_quarantine"))

    latest = Window.partitionBy("order_id").orderBy(F.desc("order_ts"))
    good = (checked.filter("reason IS NULL").drop("known", "reason")
            .withColumn("rn", F.row_number().over(latest)).filter("rn = 1").drop("rn"))
    path = f"{BASE}/silver/orders"
    if not DeltaTable.isDeltaTable(spark, path):
        good.write.format("delta").save(path)
    else:
        (DeltaTable.forPath(spark, path).alias("t")
            .merge(good.alias("s"), "t.order_id = s.order_id")
            .whenMatchedUpdateAll(condition="s.order_ts > t.order_ts")
            .whenNotMatchedInsertAll().execute())

silver_orders("2026-03-01")
print("silver_orders:", spark.read.format("delta").load(f"{BASE}/silver/orders").count(), "rows")
spark.read.format("delta").load(f"{BASE}/silver/orders_quarantine") \\
     .select("order_id", "customer_id", "qty", "reason").show()''',
         expl='''Four rules, in order: **cast** with `try_cast` so a bad quantity becomes NULL instead of failing the job; **validate** — rows with no quantity or an unknown customer go to a **quarantine** table with a reason, not into the bin; **deduplicate** — one row per `order_id`, keeping the latest; **MERGE** by key, updating only when the incoming row is newer.

27 Bronze lines became 24 Silver orders: order 1005 arrived twice with different contents, so only the later line (18:00) was kept, and two rows were quarantined for someone to fix at the source.'''),

    dict(t='15.5 Gold: daily sales by city and category (city as it was on the day)',
         code='''def gold_daily_sales():
    o = spark.read.format("delta").load(f"{BASE}/silver/orders").filter("status <> 'cancelled'").alias("o")
    c = spark.read.format("delta").load(f"{BASE}/silver/customers").alias("c")
    p = spark.read.format("delta").load(f"{BASE}/silver/products").alias("p")
    valid_then = (F.col("o.customer_id") == F.col("c.customer_id")) & \\
        F.to_date("o.order_ts").between(F.col("c.start_date"),
                                        F.coalesce(F.col("c.end_date"), F.lit("9999-12-31").cast("date")))
    (o.join(c, valid_then).join(p, F.col("o.product_id") == F.col("p.product_id"))
      .groupBy(F.to_date("o.order_ts").alias("order_date"), F.col("c.city"), F.col("p.category"))
      .agg(F.count("*").alias("orders"), F.sum("o.qty").alias("units"),
           F.sum(F.col("o.qty") * F.col("p.price")).alias("revenue"))
      .write.format("delta").mode("overwrite").save(f"{BASE}/gold/daily_sales"))

gold_daily_sales()
spark.read.format("delta").load(f"{BASE}/gold/daily_sales").orderBy("order_date", "city", "category").show(30)''',
         expl='''The join to customers is not just on `customer_id`: the order date must fall **between** the customer version's `start_date` and `end_date`. That is how SCD2 pays off — when Arjun moves on day 2, his day-1 orders stay counted under **Delhi**.

Cancelled orders are excluded. The Gold table is rebuilt from Silver on each run — it is small and always consistent.'''),

    dict(t='15.6 Gold: a customer 360 table with window functions',
         code='''def gold_customer_360():
    o = spark.read.format("delta").load(f"{BASE}/silver/orders").filter("status <> 'cancelled'")
    p = spark.read.format("delta").load(f"{BASE}/silver/products")
    cur = spark.read.format("delta").load(f"{BASE}/silver/customers").filter("is_current")
    spend = (o.join(p, "product_id")
             .groupBy("customer_id")
             .agg(F.count("*").alias("orders"),
                  F.sum(F.col("qty") * F.col("price")).alias("total_spend"),
                  F.min(F.to_date("order_ts")).alias("first_order"),
                  F.max(F.to_date("order_ts")).alias("last_order")))
    by_city = Window.partitionBy("city").orderBy(F.desc("total_spend"))
    (cur.select("customer_id", "name", "city").join(spend, "customer_id", "left")
        .fillna({"orders": 0, "total_spend": 0.0})
        .withColumn("rank_in_city", F.dense_rank().over(by_city))
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .save(f"{BASE}/gold/customer_360"))

gold_customer_360()
spark.read.format("delta").load(f"{BASE}/gold/customer_360").orderBy("city", "rank_in_city").show()''',
         expl='''One row per **current** customer: number of orders, total spend, first and last order date, and the customer's rank by spend inside their city (`dense_rank` over a window). A left join keeps customers who have not ordered yet, with zeros.

This is the kind of table a CRM screen, a marketing campaign or a churn model (Chapter 16) reads.'''),

    dict(t='15.7 The whole day as one function — then day 2 arrives',
         code='''def run_day(day):
    land(day)
    silver_customers(day)
    silver_orders(day)
    gold_daily_sales()
    gold_customer_360()
    return {t: spark.read.format("delta").load(f"{BASE}/{t}").count()
            for t in ["bronze/orders", "silver/orders", "silver/orders_quarantine",
                      "silver/customers", "gold/daily_sales", "gold/customer_360"]}

for table, rows in run_day("2026-03-02").items():
    print(f"{table:28} {rows:3} rows")''',
         expl='''The daily job is just the steps in order. A scheduler (Airflow, Databricks Workflows) calls `run_day` with the date; a failed day is re-run with the same date.

Day 2 added 15 Bronze order lines (14 new orders and one correction), 14 new Silver orders, and new Silver customer rows for the move and the new customer. The products step is not repeated: the catalogue did not change.'''),

    dict(t='15.8 Check what day 2 changed in Silver',
         code='''spark.read.format("delta").load(f"{BASE}/silver/customers") \\
     .filter("customer_id IN ('C03', 'C07')").orderBy("customer_id", "start_date").show()

spark.read.format("delta").load(f"{BASE}/silver/orders") \\
     .filter("order_id = 1003").select("order_id", "order_ts", "customer_id", "status").show()''',
         expl='''**SCD2 worked**: Arjun (C03) has a closed Delhi row ending 2026-03-01 and a current Bengaluru row from 2026-03-02; Dev (C07) is new.

**The correction worked**: order 1003 is now `cancelled`, because the day-2 row had a later timestamp, so the MERGE's `whenMatchedUpdateAll(condition="s.order_ts > t.order_ts")` applied it. It disappears from Gold revenue automatically.'''),

    dict(t='15.9 Gold after two days — and the same table yesterday (time travel)',
         code='''gold = f"{BASE}/gold/daily_sales"
today = spark.read.format("delta").load(gold)
yesterday = spark.read.format("delta").option("versionAsOf", 0).load(gold)

(today.groupBy("order_date").agg(F.sum("revenue").alias("revenue_now"))
      .join(yesterday.groupBy("order_date").agg(F.sum("revenue").alias("revenue_v0")), "order_date", "left")
      .orderBy("order_date").show())

today.filter("city = 'Delhi' OR city = 'Bengaluru'").orderBy("order_date", "city").show()''',
         expl='''Day-1 revenue went **down** between version 0 and now: the cancelled order 1003 was removed. Time travel shows exactly by how much — the kind of check an analyst asks for when "yesterday's number changed".

In the second table, Delhi still has day-1 sales (Arjun's orders before the move) and Bengaluru gets his day-2 sales.'''),

    dict(t='15.10 Quality gates across the layers',
         code='''def layer_checks():
    so = spark.read.format("delta").load(f"{BASE}/silver/orders")
    sc = spark.read.format("delta").load(f"{BASE}/silver/customers")
    checks = {
        "silver_orders: duplicate order_id": so.count() - so.select("order_id").distinct().count(),
        "silver_orders: qty <= 0 or NULL": so.filter("qty IS NULL OR qty <= 0").count(),
        "silver_customers: >1 current row": sc.filter("is_current").groupBy("customer_id").count()
                                              .filter("count > 1").count(),
        "gold vs silver revenue gap": round(
            spark.read.format("delta").load(f"{BASE}/gold/daily_sales").agg(F.sum("revenue")).first()[0]
            - so.filter("status <> 'cancelled'").join(spark.read.format("delta").load(f"{BASE}/silver/products"), "product_id")
                .agg(F.sum(F.col("qty") * F.col("price"))).first()[0], 2),
    }
    for name, value in checks.items():
        print(f"{'OK  ' if value == 0 else 'FAIL'} {name:38} {value}")
    assert all(v == 0 for v in checks.values()), "quality gate failed"

layer_checks()''',
         expl='''Every check should be **zero**: no duplicate keys, no invalid quantities, exactly one current version per customer, and Gold revenue that reconciles to Silver to the rupee. The job raises if any check fails, so a bad day never reaches the dashboard silently.

Add these checks to `run_day` and you have the capstone: a re-runnable, tested, history-keeping lakehouse pipeline.'''),
]

POINTS = '''
1. **Bronze**: append raw files as text with `_source_file` and `_ingest_date`; never reject anything.
2. **Silver**: cast with `try_cast`, validate, **quarantine** failures with a reason, deduplicate with `row_number`, **MERGE** by key (update only when newer).
3. Customers are an **SCD2** dimension: close the old version, insert the new — so facts can be joined to the version valid on their date.
4. **Gold**: rebuild from Silver; join facts to SCD2 dimensions **by key and date range**; exclude cancelled orders.
5. Wrap each layer in a function and the day in `run_day(day)` — schedulable, re-runnable, testable.
6. **Quality gates** that must be zero (duplicates, invalid values, one current row, Gold reconciles to Silver) stop bad data before it reaches users.
7. Delta **time travel** explains why a number changed between yesterday and today.

    | Table | Layer | Write pattern |
    |---|---|---|
    | bronze/orders, bronze/customers | Bronze | append per day |
    | silver/customers | Silver | SCD2 MERGE |
    | silver/orders | Silver | MERGE (upsert, newer wins) |
    | silver/orders_quarantine | Silver | append |
    | gold/daily_sales, gold/customer_360 | Gold | overwrite from Silver |
'''

MCQS = [
    dict(q='Order 1025 has the quantity "two". Where does it end up?', kind='concept',
         o=['It is dropped silently', 'In Bronze as text, and in the Silver quarantine table with a reason', 'In Silver orders with qty = 2', 'The job fails'], a=1,
         why='Bronze keeps everything; try_cast makes qty NULL, and the validation rule routes the row to quarantine with "qty is not a number".'),
    dict(q='Why does the Gold daily-sales join use the order date between start_date and end_date?', kind='concept',
         o=['To make the join faster', 'So each order is matched to the customer version (city) valid on the order date', 'Because customer_id is not unique', 'To remove cancelled orders'], a=1,
         why='With SCD2 a customer has several versions; the date-range condition picks the one in force when the order was placed.'),
    dict(q='The day-2 file contains order 1003 again with status cancelled and a later timestamp. What does the Silver MERGE do?', kind='concept',
         o=['Inserts a duplicate row', 'Updates the existing row because the incoming order_ts is newer', 'Ignores it', 'Deletes the order'], a=1,
         why='whenMatchedUpdateAll(condition="s.order_ts > t.order_ts") applies newer versions of an existing order.'),
    dict(q='`run_day("2026-03-02")` crashes after the Bronze step and is re-run. Which part could create duplicates?', kind='concept',
         o=['Silver orders — it MERGEs by key', 'Gold — it overwrites', 'Bronze — it appends the same files again', 'None of them'], a=2,
         why='Append-only Bronze would land the day twice. Real pipelines guard it: overwrite the day\'s partition (replaceWhere / dynamic overwrite) or record loaded files — Silver dedup then keeps results correct.'),
    dict(q='Which check proves Gold is consistent with Silver?', kind='concept',
         o=['Both tables have the same number of rows', 'Gold total revenue equals the revenue computed directly from non-cancelled Silver orders', 'Gold has no NULLs', 'Silver has no duplicates'], a=1,
         why='Reconciling an aggregate back to its source is the standard Gold-layer test.'),
]

PRACTICE = dict(
    kinds=['Make Bronze landing idempotent: re-running a day must not duplicate rows (hint: `replaceWhere` on `_ingest_date`).',
           'Add a day 3 with a returned order and a product price change; make products SCD2 and use the price valid on the order date.',
           'Write pytest tests for `silver_orders` (Chapter 14) with hand-made Bronze rows covering every quarantine reason.',
           'Turn the orders landing into Structured Streaming with `availableNow` (Chapter 13) instead of a daily batch read.',
           'Put the capstone on GitHub: README with the layer diagram, the table list above, and the physical plan of `gold_daily_sales`.'],
    note='This is the project to show in interviews: it uses every chapter of the course.')
