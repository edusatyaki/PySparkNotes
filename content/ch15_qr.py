ORDER = 15
KEY = 'QR'
TITLE = 'The Whole Course on One Page: SQL ↔ PySpark'
WEEK = 'Quick reference'

SECTIONS = [
    dict(t='Session, reading and writing', md='''| Task | PySpark |
|---|---|
| Start Spark | `spark = SparkSession.builder.appName("job").getOrCreate()` |
| Read CSV | `spark.read.option("header", True).schema("a INT, b STRING").csv(path)` |
| Read Parquet / JSON | `spark.read.parquet(path)` · `spark.read.json(path)` |
| Write | `df.write.mode("overwrite").partitionBy("day").parquet(path)` |
| Look | `df.show(n, truncate=False)` · `df.printSchema()` · `df.explain()` |
| To / from pandas | `df.toPandas()` · `spark.createDataFrame(pdf)` |'''),
    dict(t='SELECT, WHERE, ORDER BY', md='''| SQL | PySpark |
|---|---|
| `SELECT a, b * 2 AS c` | `df.select("a", (F.col("b") * 2).alias("c"))` |
| `WHERE x > 5 AND y = 'A'` | `df.filter((F.col("x") > 5) & (F.col("y") == "A"))` |
| `WHERE z IS NULL` | `df.filter(F.col("z").isNull())` |
| `WHERE c IN (1, 2)` | `df.filter(F.col("c").isin(1, 2))` |
| `WHERE n LIKE 'A%'` | `df.filter(F.col("n").like("A%"))` |
| `SELECT DISTINCT a` | `df.select("a").distinct()` |
| `ORDER BY a DESC NULLS LAST` | `df.orderBy(F.col("a").desc_nulls_last())` |
| `LIMIT 10` | `df.limit(10)` |
| `CASE WHEN … THEN … ELSE … END` | `F.when(cond, v).when(…).otherwise(d)` |
| `COALESCE(a, b)` | `F.coalesce("a", "b")` |
| `CAST(a AS INT)` | `F.col("a").cast("int")` (`try_cast` to avoid errors) |'''),
    dict(t='GROUP BY, joins, set operations', md='''| SQL | PySpark |
|---|---|
| `GROUP BY a` + `SUM(b)` | `df.groupBy("a").agg(F.sum("b").alias("s"))` |
| `HAVING SUM(b) > 10` | `….agg(F.sum("b").alias("s")).filter("s > 10")` |
| `COUNT(DISTINCT b)` | `F.countDistinct("b")` |
| `SUM(CASE WHEN c THEN x END)` | `F.sum(F.when(c, F.col("x")))` |
| `INNER / LEFT / FULL JOIN … USING (k)` | `a.join(b, "k", "inner" / "left" / "full")` |
| `WHERE EXISTS (…)` | `a.join(b, "k", "left_semi")` |
| `WHERE NOT EXISTS (…)` | `a.join(b, "k", "left_anti")` |
| `UNION ALL` / `UNION` | `a.unionByName(b)` / `….distinct()` |
| `INTERSECT` / `EXCEPT` | `a.intersect(b)` / `a.subtract(b)` |'''),
    dict(t='Window functions', md='''| SQL | PySpark |
|---|---|
| `OVER (PARTITION BY p ORDER BY o)` | `w = Window.partitionBy("p").orderBy("o")` |
| `ROW_NUMBER() / RANK() / DENSE_RANK()` | `F.row_number().over(w)` · `F.rank()` · `F.dense_rank()` |
| `LAG(x) / LEAD(x)` | `F.lag("x").over(w)` · `F.lead("x").over(w)` |
| Running total | `F.sum("x").over(w.rowsBetween(Window.unboundedPreceding, 0))` |
| Last 3 rows | `w.rowsBetween(-2, 0)` |
| Last 7 days | order by `ts.cast("long")`, `rangeBetween(-7 * 86400, 0)` |
| Top N per group | `withColumn("rn", F.row_number().over(w)).filter("rn <= N")` |'''),
    dict(t='Functions you will use every day', md='''| Area | Functions |
|---|---|
| Strings | `trim`, `lower`, `upper`, `initcap`, `length`, `concat_ws`, `split`, `substring`, `regexp_replace`, `regexp_extract`, `lpad` |
| Dates | `to_date`, `date_format`, `year`, `month`, `datediff`, `months_between`, `date_add`, `add_months`, `date_trunc`, `last_day` |
| Numbers | `round`, `bround`, `floor`, `ceil`, `abs`, `greatest`, `least`, `try_divide` |
| NULLs | `isNull`, `isNotNull`, `coalesce`, `fillna`, `dropna`, `nullif`, `eqNullSafe` |
| Arrays / maps | `explode(_outer)`, `size`, `array_contains`, `collect_list`, `collect_set`, `transform`, `filter` |
| JSON | `from_json`, `to_json`, `get_json_object` |'''),
    dict(t='Performance checklist', md='''| Check | How |
|---|---|
| How many shuffles? | Count `Exchange` in `df.explain()` |
| Is the small table broadcast? | Look for `BroadcastHashJoin`; else `F.broadcast(small)` |
| Are filters pushed down? | `PushedFilters` / `PartitionFilters` in the scan |
| Is a task much slower than the rest? | Spark UI stage page → skew → salt / AQE |
| Too many tiny files or tasks? | AQE on; `coalesce(n)` before writing |
| Python UDF in a hot path? | Replace with built-ins or a pandas UDF |
| Result reused several times? | `cache()` … `unpersist()` |'''),
]
