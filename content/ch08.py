ORDER = 8
KEY = '8'
TITLE = 'Spark SQL: views, the catalog and mixing SQL with Python'
WEEK = 'Week 4'
DATA = ['students', 'marks']

SETUP_TITLE = '8.0 The data — and a view for SQL'
SETUP = '''from pyspark.sql import functions as F

students = spark.createDataFrame([
    (1, "Amit", "CSE", "Pune"), (2, "Riya", "ECE", "Mumbai"), (3, "Arjun", "CSE", "Delhi"),
    (4, "Sneha", "ME", "Pune"), (5, "Ankit", "CSE", "Kolkata"), (6, "Neha", "ECE", "Pune"),
], "student_id INT, name STRING, branch STRING, city STRING")

marks = spark.createDataFrame([
    (1, "DBMS", 85), (1, "OS", 78), (2, "DBMS", 72), (2, "OS", 91), (3, "DBMS", 95),
    (3, "OS", 88), (4, "DBMS", 58), (5, "OS", 66), (6, "DBMS", 81), (6, "OS", 74),
], "student_id INT, subject STRING, score INT")

students.createOrReplaceTempView("students")
marks.createOrReplaceTempView("marks")'''
SETUP_EXPL = '''`createOrReplaceTempView(name)` registers a DataFrame under a name that SQL can use. Nothing is copied or computed — a view is just a name for the DataFrame's plan.

A temporary view lives as long as this SparkSession and is visible only to it.'''

SECTIONS = [
    dict(t='8.1 Run SQL and get a DataFrame back', tables=['students'], home=True,
         code='''top = spark.sql("""
    SELECT s.name, s.branch, ROUND(AVG(m.score), 1) AS avg_score
    FROM students s JOIN marks m ON s.student_id = m.student_id
    GROUP BY s.name, s.branch
    ORDER BY avg_score DESC
""")
top.show()
print(type(top))''',
         expl='''`spark.sql(query)` returns an ordinary **DataFrame**. It is lazy like any other — `show()` runs it.

SQL and the DataFrame API compile to the **same plan** through the same optimiser, so there is no speed difference. Choose whichever reads better for the task.'''),

    dict(t='8.2 Mix both: SQL for the query, Python for the rest', tables=['students', 'marks'],
         code='''dbms = spark.sql("SELECT student_id, score FROM marks WHERE subject = 'DBMS'")

result = (dbms.join(students, "student_id")
              .withColumn("grade", F.when(F.col("score") >= 80, "A").otherwise("B"))
              .orderBy(F.desc("score")))
result.select("name", "score", "grade").show()''',
         expl='''Because `spark.sql` returns a DataFrame, you can keep transforming it with Python methods — and register any DataFrame as a view to continue in SQL. Teams often write the business logic in SQL and the plumbing (reading, writing, loops over dates) in Python.'''),

    dict(t='8.3 CTEs and subqueries work as in PostgreSQL', tables=['students', 'marks'],
         code='''spark.sql("""
    WITH avg_by_subject AS (
        SELECT subject, AVG(score) AS subj_avg FROM marks GROUP BY subject
    )
    SELECT s.name, m.subject, m.score, ROUND(a.subj_avg, 1) AS subj_avg
    FROM marks m
    JOIN avg_by_subject a ON m.subject = a.subject
    JOIN students s ON s.student_id = m.student_id
    WHERE m.score > a.subj_avg
    ORDER BY m.subject, m.score DESC
""").show()''',
         expl='''`WITH` (CTE), subqueries in `WHERE` / `FROM`, `EXISTS`, `IN`, window functions and `CASE WHEN` all work. Spark SQL follows ANSI SQL closely, so most of your PostgreSQL queries run unchanged.

This query lists every score above its subject's average.'''),

    dict(t='8.4 Window functions in SQL', tables=['marks'],
         code='''spark.sql("""
    SELECT * FROM (
        SELECT student_id, subject, score,
               DENSE_RANK() OVER (PARTITION BY subject ORDER BY score DESC) AS rnk
        FROM marks
    ) WHERE rnk <= 2
    ORDER BY subject, rnk
""").show()''',
         expl='''The same top-N-per-group pattern as Chapter 7, in SQL: rank inside a subquery, filter outside it. Many people find window functions easier to read in SQL.'''),

    dict(t='8.5 Pass values safely: named parameters', tables=['marks'],
         code='''spark.sql(
    "SELECT student_id, score FROM marks WHERE subject = :subj AND score >= :min_score",
    args={"subj": "OS", "min_score": 75},
).show()''',
         expl='''`args={...}` binds values to `:name` markers (Spark 3.4+). The values are passed as data, never pasted into the SQL text, so a value like `"OS' OR 1=1 --"` cannot change the query — the same reason you use parameters in PostgreSQL drivers.

Never build SQL with f-strings from user input.'''),

    dict(t='8.6 Reference a DataFrame without a view', tables=['students'],
         code='''pune = students.filter(F.col("city") == "Pune")

spark.sql("SELECT branch, COUNT(*) AS n FROM {df} GROUP BY branch ORDER BY branch",
          df=pune).show()''',
         expl='''Keyword arguments to `spark.sql` that are DataFrames can be referenced as `{name}` in the query (Spark 3.3+). Spark registers them for this one query — no temp views left behind.'''),

    dict(t='8.7 What is in the catalog?',
         code='''for t in spark.catalog.listTables():
    print(t.name, t.tableType, t.isTemporary)

print(spark.catalog.currentDatabase())
spark.sql("SHOW TABLES").show()''',
         expl='''`spark.catalog` lists databases, tables, views and functions. Our two views appear as `TEMPORARY`.

The **catalog** (also called the metastore) stores table names, schemas and file locations. Locally Spark uses an in-memory catalog; on a cluster it is usually a Hive Metastore, AWS Glue or Unity Catalog.'''),

    dict(t='8.8 A permanent table: saveAsTable', tables=['marks'],
         code='''marks.write.mode("overwrite").saveAsTable("marks_table")

spark.sql("DESCRIBE TABLE marks_table").show()
print(spark.table("marks_table").count())''',
         expl='''`saveAsTable` writes the data as files (Parquet by default) into the **warehouse directory** (`spark-warehouse/` locally) and registers it in the catalog. Unlike a temp view, the data itself is stored and outlives the session.

`spark.table(name)` reads a table or view back as a DataFrame.'''),

    dict(t='8.9 Global temp views are shared across sessions',
         code='''students.createOrReplaceGlobalTempView("students_g")

other = spark.newSession()
print(other.sql("SELECT COUNT(*) FROM global_temp.students_g").first()[0])
print([t.name for t in other.catalog.listTables()])''',
         expl='''A **global** temp view lives in the special `global_temp` database and is visible to every session of the same Spark application. A normal temp view is not: the new session sees only the permanent `marks_table` from the previous slide, not the `students` and `marks` temp views.'''),

    dict(t='8.10 SQL functions on arrays: higher-order functions',
         code='''spark.sql("""
    SELECT student_id,
           collect_list(score) AS scores,
           aggregate(collect_list(score), 0, (acc, x) -> acc + x) AS total,
           filter(collect_list(score), x -> x >= 80) AS high
    FROM marks GROUP BY student_id ORDER BY student_id
""").show()''',
         expl='''Spark SQL has lambdas written as `x -> expression`: `transform`, `filter`, `exists`, `aggregate`. They are the SQL form of the higher-order functions from Chapter 5 and run in the JVM.'''),
]

POINTS = '''
1. `df.createOrReplaceTempView("name")` → query it with `spark.sql("…")`. Views store no data.
2. `spark.sql` returns a DataFrame — chain Python methods on it, or register results as new views.
3. SQL and the DataFrame API produce the **same plan**: choose by readability, not speed.
4. Use parameters: `spark.sql("… WHERE x = :v", args={"v": 5})`. Never f-string user input into SQL.
5. `spark.sql("SELECT … FROM {df}", df=my_df)` uses a DataFrame without creating a view.

    | Object | Lives | Visible to | Stores data? |
    |---|---|---|---|
    | Temp view | Session | That session | No |
    | Global temp view (`global_temp.x`) | Application | All sessions | No |
    | Table (`saveAsTable`) | Until dropped | Everyone using the catalog | Yes (files) |

6. `spark.catalog` / `SHOW TABLES` / `DESCRIBE TABLE` inspect the metastore.
'''

EXTRA_TITLE = 'Deep dive: one optimiser for both languages'
EXTRA_LEAD = 'Proof that SQL and Python end up as the same plan.'
EXTRA = [
    dict(t='Same query, same physical plan', tables=['marks'],
         code='''q_sql = spark.sql("SELECT subject, AVG(score) AS a FROM marks WHERE score > 70 GROUP BY subject")
q_df = marks.filter("score > 70").groupBy("subject").agg(F.avg("score").alias("a"))

q_sql.explain()
q_df.explain()''',
         expl='''Both print the same operators — Filter → partial HashAggregate → Exchange → final HashAggregate — down to the column IDs (`#5`, `#6`). Only the internal `plan_id` differs.

Catalyst parses SQL into the same logical plan a DataFrame builds, then optimises both identically.'''),
    dict(t='The four stages Catalyst goes through', tables=['marks'],
         code='''spark.sql("SELECT subject, score * 2 AS s2 FROM marks WHERE score > 80 AND 1 = 1").explain("extended")''',
         expl='''`explain("extended")` shows each step: the **parsed** plan (names not yet checked), the **analyzed** plan (names resolved against the catalog, types known), the **optimized** plan (rules applied — the useless `1 = 1` is gone and filters are pushed down), and the **physical** plan (the operators that actually run).'''),
]

MCQS = [
    dict(q='What does `spark.sql("SELECT …")` return?', kind='concept',
         o=['A list of Row objects', 'A pandas DataFrame', 'A Spark DataFrame (lazy)', 'The number of rows'], a=2,
         why='spark.sql returns a lazy DataFrame; an action such as show or collect runs it.'),
    dict(q='Which is faster: the SQL version of a query or the DataFrame API version?', kind='concept',
         o=['SQL, because it is compiled', 'The DataFrame API, because it is Python', 'Neither — both go through Catalyst to the same plan', 'It depends on the number of columns'], a=2,
         why='Both are optimised by Catalyst into the same physical plan.'),
    dict(q='A temp view was created in one session. Another session (`spark.newSession()`) queries it. What happens?', kind='concept',
         o=['It works', 'It fails: temp views are visible only to the session that created them', 'It returns an empty result', 'It copies the data'], a=1,
         why='Temp views are session-scoped. Use a global temp view (global_temp.name) to share within the application.'),
    dict(q='What does this print?', kind='output', run=True, check='2',
         code='''spark.createDataFrame([(1,), (2,), (3,)], "x INT").createOrReplaceTempView("t")
print(spark.sql("SELECT COUNT(*) FROM t WHERE x > :v", args={"v": 1}).first()[0])''',
         o=['1', '2', '3', '0'], a=1,
         why='The parameter v = 1 is bound safely; two rows have x > 1.'),
    dict(q='Why is this code dangerous?', kind='debug',
         code='''city = request.args["city"]
spark.sql(f"SELECT * FROM customers WHERE city = '{city}'")''',
         o=['f-strings are slow', 'User input pasted into SQL allows SQL injection — use args={...} parameters', 'spark.sql cannot use quotes', 'city must be upper case'], a=1,
         why='A value like `x\' OR \'1\'=\'1` changes the query. Bind values with named parameters instead.'),
    dict(q='Where does `saveAsTable("t")` store the data when running locally with default settings?', kind='concept',
         o=['Only in memory', 'In the spark-warehouse directory as Parquet files', 'In PostgreSQL', 'It does not store data'], a=1,
         why='saveAsTable writes files to the warehouse directory (spark-warehouse by default) and registers the table in the catalog.'),
    dict(q='In `explain("extended")`, which plan has the redundant `AND 1 = 1` removed?', kind='concept',
         o=['Parsed logical plan', 'Analyzed logical plan', 'Optimized logical plan', 'None — Spark keeps it'], a=2,
         why='Optimisation rules such as constant folding and boolean simplification run on the optimized logical plan.'),
]

PRACTICE = dict(
    kinds=['Run your existing PostgreSQL practice queries unchanged with spark.sql, and note any that fail.',
           'Write each answer twice — SQL and DataFrame API — and compare the plans with explain().',
           'Parameterise every filter value with args={...}.'],
    problems=[
        (1068, 'product-sales-analysis-i', 'Product Sales Analysis I', 'Easy', 'Plain SQL join through a temp view.'),
        (1075, 'project-employees-i', 'Project Employees I', 'Easy', 'Join + GROUP BY + ROUND(AVG).'),
        (1251, 'average-selling-price', 'Average Selling Price', 'Easy', 'Range join on dates, weighted average.'),
        (1633, 'percentage-of-users-attended-a-contest', 'Percentage of Users Attended a Contest', 'Easy', 'Scalar subquery for the total.'),
        (1164, 'product-price-at-a-given-date', 'Product Price at a Given Date', 'Medium', 'CTE + window function in SQL.'),
    ])
