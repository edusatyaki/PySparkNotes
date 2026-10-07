ORDER = 2
KEY = '2'
TITLE = 'DataFrame Basics and spark.sql: select, filter, withColumn, orderBy'
WEEK = 'Week 2'
DATA = ['students']

SETUP_TITLE = '2.0 The data — with an explicit schema'
SETUP = '''from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("branch", StringType(), True),
    StructField("city", StringType(), True),
    StructField("marks", IntegerType(), True),
    StructField("attendance", IntegerType(), True),
])

students = spark.createDataFrame([
    (1, "Amit", "CSE", "Pune", 85, 88),     (2, "Riya", "ECE", "Mumbai", 72, 95),
    (3, "Arjun", "CSE", "Delhi", 91, 70),   (4, "Sneha", "ME", "Pune", 67, 80),
    (5, "Ankit", "CSE", "Kolkata", 88, 92), (6, "Asha", "ECE", "Delhi", 76, 84),
    (7, "Neha", "ECE", "Pune", 79, 91),     (8, "Rahul", "CSE", "Mumbai", 65, 74),
    (9, "Priya", "ME", "Delhi", 82, 87),    (10, "Vikram", "CSE", "Jaipur", 94, 68),
    (11, "Isha", "ECE", "Pune", 58, 90),    (12, "Karan", "ME", "Kolkata", 71, 77),
    (13, "Meera", "CSE", "Chennai", 88, 93),(14, "Rohan", "ECE", "Delhi", 63, 82),
    (15, "Tanya", "CSE", "Mumbai", 90, 85),
], schema)'''
SETUP_EXPL = '''A **`StructType`** is the schema written by hand: one `StructField(name, type, nullable)` per column. With an explicit schema Spark does not guess — `marks` is a 32-bit `IntegerType` instead of the inferred `long`.

In production, always give an explicit schema when you read files: inference costs an extra pass over the data and can guess wrong (a ZIP code read as a number loses its leading zero).'''

SECTIONS = [
    dict(t='2.1 Pick columns: three ways to name a column', tables=['students'],
         code='''students.select("name", "marks").show(3)
students.select(F.col("name"), F.col("marks")).show(3)
students.select(students.name, students["marks"]).show(3)''',
         expl='''All three give the same result. A **string** is shortest; **`F.col("x")`** builds a Column object you can do maths and comparisons on; **`df.x`** / **`df["x"]`** ties the column to one DataFrame, which matters after a join when two tables share a column name.

Pick one style and stay with it. `F.col` is the most common in production code.'''),

    dict(t='2.2 Add a computed column (withColumn)', tables=['students'],
         code='''scored = students.withColumn("percentage", F.col("marks") / 100)
scored.select("name", "marks", "percentage").show(5)''',
         expl='''`withColumn(name, expression)` returns a **new** DataFrame with one more column (or replaces a column if the name already exists). The original `students` is unchanged — DataFrames are **immutable**.

Dividing two integers gives a `double`, like Python 3 — unlike PostgreSQL, where `85 / 100` is `0`.'''),

    dict(t='2.3 Rename columns: withColumnRenamed and alias', tables=['students'],
         code='''students.withColumnRenamed("marks", "score").printSchema()

students.select(F.col("name").alias("student"),
                (F.col("marks") + 5).alias("with_grace")).show(4)''',
         expl='''`withColumnRenamed(old, new)` renames in place and keeps every other column. `alias()` names an expression inside `select` — the PySpark version of SQL `AS`.

Without an alias, `F.col("marks") + 5` would get the ugly generated name `(marks + 5)`.'''),

    dict(t='2.4 Remove columns (drop)', tables=['students'],
         code='''slim = students.drop("attendance", "city")
print(slim.columns)

print(students.drop("no_such_column").columns == students.columns)''',
         expl='''`drop` takes one or more names and returns the DataFrame without them. Dropping a column that does not exist is **silently ignored** — no error — so a typo in `drop` goes unnoticed. Check `columns` after dropping.'''),

    dict(t='2.5 Only the CSE students (filter / where)', tables=['students'], home=True,
         code='''students.filter(F.col("branch") == "CSE").show()

# where() is an alias of filter(); a SQL string also works
print(students.where("branch = 'CSE'").count())''',
         expl='''`filter(condition)` keeps the rows where the condition is true. Comparisons use Python operators: `==`, `!=`, `>`, `>=`, `<`, `<=`.

`where` is exactly the same method under the SQL name, and both accept a SQL condition string. Use whichever reads better.'''),

    dict(t='2.6 Combine conditions: &, | and ~', tables=['students'],
         code='''students.filter(
    (F.col("branch") == "CSE") & (F.col("city") == "Pune")
    | (F.col("marks") > 90)
).show()

print(students.filter(~(F.col("city") == "Delhi")).count())''',
         expl='''Use **`&`** (and), **`|`** (or) and **`~`** (not) — not Python's `and`/`or`/`not`, which cannot combine Column objects.

**Wrap every comparison in parentheses.** In Python `&` binds tighter than `==`, so `F.col("branch") == "CSE" & F.col("city") == "Pune"` is parsed wrongly and fails. Here `&` is evaluated before `|`, giving: (CSE and Pune) or marks > 90 — Amit, plus Arjun and Vikram. The second query counts the 11 students who are not from Delhi.'''),

    dict(t='2.7 Lists and ranges: isin and between', tables=['students'],
         code='''students.filter(F.col("city").isin("Pune", "Mumbai")).select("name", "city").show()

students.filter(F.col("marks").between(80, 90)).select("name", "marks").show()''',
         expl='''`isin(...)` is SQL `IN`; it takes values or a Python list (`isin(cities)`). `between(low, high)` is **inclusive** at both ends, like SQL — 80 and 90 both qualify, so Meera and Ankit (88) and Tanya (90) are in.'''),

    dict(t='2.8 Text patterns: like, endswith, rlike', tables=['students'],
         code='''students.filter(F.col("name").like("A%")).select("name").show()
students.filter(F.col("name").endswith("a")).select("name").show(3)
students.filter(F.col("name").rlike("^[AEIOU].*[aeiou]$")).select("name").show()''',
         expl='''`like` uses SQL wildcards: `%` any characters, `_` exactly one. `startswith`, `endswith` and `contains` do simple checks without wildcards. `rlike` takes a **regular expression** — here: starts with a capital vowel and ends with a small vowel.

Use `ilike` for a case-insensitive `like` (Spark 3.3+).'''),

    dict(t='2.9 Sort: orderBy, asc and desc', tables=['students'],
         code='''(students.orderBy(F.col("branch").asc(), F.col("marks").desc())
         .select("name", "branch", "marks")
         .show())''',
         expl='''`orderBy` (alias `sort`) takes columns with `.asc()` / `.desc()`. Here branches go A→Z and, inside each branch, marks go high→low.

Sorting the whole DataFrame is a **wide** operation: Spark samples the data, range-partitions it and sorts each partition. Sort only when you need ordered output.''',
         tip='NULLs come first in ascending order and last in descending order. Use `asc_nulls_last()` or `desc_nulls_first()` to choose.'),

    dict(t='2.10 Remove duplicates: distinct and dropDuplicates', tables=['students'],
         code='''students.select("branch").distinct().orderBy("branch").show()

(students.dropDuplicates(["branch"])
         .select("branch", "name").orderBy("branch").show())''',
         expl='''`distinct()` removes rows that are identical in **every** selected column. `dropDuplicates([cols])` keeps **one whole row** for each distinct value of the listed columns.

Which row survives `dropDuplicates` is not guaranteed — it depends on partitioning. To keep a specific row (the latest, the highest), use a window function with `row_number` (Chapter 7).'''),

    dict(t='2.11 Show it the way you need: n, truncate, vertical', tables=['students'],
         code='''students.limit(2).show(vertical=True)

notes = spark.createDataFrame([("A very long remark that goes on and on",)], ["remark"])
notes.show()
notes.show(truncate=False)''',
         expl='''`show(n)` prints n rows (default 20). Strings longer than 20 characters are cut with `...` unless you pass `truncate=False`. `vertical=True` prints one block per row — useful for wide tables.

`limit(n)` is a transformation that returns a smaller DataFrame; `show(n)` only prints.'''),

    dict(t='2.12 Write SQL inside the DataFrame API: selectExpr and expr', tables=['students'],
         code='''students.selectExpr("name", "marks * 1.1 AS boosted",
                    "upper(city) AS city").show(4)

students.withColumn("pass", F.expr("marks >= 70 AND attendance >= 75")).show(4)''',
         expl='''`selectExpr` takes SQL expression strings; `F.expr` turns one SQL string into a Column you can use anywhere. Handy when an expression is shorter in SQL, or when it comes from a config file.

Both go through the same optimiser as `F.col` code, so there is no speed difference.'''),

    dict(t='2.13 Several columns at once: withColumns and lit', tables=['students'],
         code='''report = students.withColumns({
    "grade_points": F.round(F.col("marks") / 10, 1),
    "batch": F.lit("2026"),
    "eligible": F.col("attendance") >= 75,
})
report.select("name", "grade_points", "batch", "eligible").show(5)''',
         expl='''`withColumns` (Spark 3.3+) adds or replaces several columns from one dict — one plan step instead of a chain of `withColumn` calls.

`F.lit(value)` wraps a plain Python value as a Column. You need it whenever a constant has to be a column — a fixed label, a default, a flag.''',
         tip='Calling withColumn in a long Python loop (hundreds of times) builds a huge plan and slows planning. Build the expressions in a list and use one `select` or `withColumns`.'),

    dict(t='2.14 Change a type (cast)', tables=['students'],
         code='''casted = students.select(
    "name",
    F.col("marks").cast("double").alias("marks_d"),
    F.col("id").cast("string").alias("id_s"),
    F.col("marks").cast("string").cast("int").alias("round_trip"),
)
casted.printSchema()
casted.show(3)''',
         expl='''`cast(type)` converts a column; the type can be a string (`"int"`, `"double"`, `"date"`) or a type object (`IntegerType()`). Casts can be chained.

Spark 4 runs in **ANSI mode**: a cast that cannot succeed (the text `"abc"` to `int`) raises an error instead of returning NULL. The deep-dive slides at the end of this chapter show the error and the `try_cast` fix.'''),

    dict(t='2.15 The schema as a short DDL string',
         code='''people = spark.createDataFrame(
    [(1, "Amit", 85.5), (2, "Riya", None)],
    "id INT, name STRING, score DOUBLE")

people.printSchema()
people.show()''',
         expl='''Instead of a `StructType` you can write the schema as a **DDL string** — the same syntax as SQL `CREATE TABLE`. It is shorter and works in `createDataFrame`, `spark.read.schema(...)` and `from_json`.

Python `None` becomes `NULL` in Spark.'''),

    dict(divider=True, t='The same questions in SQL: spark.sql',
         lead='Every query in this chapter can also be written as SQL. Register the DataFrame as a view once, then use whichever language reads better.'),

    dict(t='2.16 Give the DataFrame a name SQL can use', tables=['students'],
         code='''students.createOrReplaceTempView("students")

spark.sql("""
    SELECT name, branch, marks
    FROM students
    WHERE branch = 'CSE' AND marks > 85
    ORDER BY marks DESC
""").show()''',
         expl='''`createOrReplaceTempView("students")` registers the DataFrame under a name for SQL — nothing is copied. `spark.sql(query)` returns an ordinary DataFrame, so `show()` runs it.

This is the same query as `students.filter((F.col("branch") == "CSE") & (F.col("marks") > 85)).orderBy(F.desc("marks"))`, and it compiles to the **same plan**.'''),

    dict(t='2.17 SQL in, DataFrame out — and back again', tables=['students'],
         code='''toppers = spark.sql("SELECT * FROM students WHERE marks >= 85")

report = (toppers.withColumn("grade", F.when(F.col("marks") >= 90, "A+").otherwise("A"))
                 .select("name", "city", "marks", "grade")
                 .orderBy("name"))
report.createOrReplaceTempView("toppers_report")

spark.sql("SELECT grade, COUNT(*) AS n FROM toppers_report GROUP BY grade ORDER BY grade").show()''',
         expl='''Mix freely: start in SQL, continue with DataFrame methods, register the result as another view, and finish in SQL. Teams often write business rules in SQL and the plumbing (reading, writing, loops over dates) in Python.

Chapter 9 goes deeper: CTEs, parameters, the catalog and permanent tables.'''),
]

POINTS = '''
1. **DataFrames are immutable**: every transformation returns a new DataFrame; assign it to a variable or chain the calls.
2. Name columns with a string, `F.col("x")` or `df.x`. Use `df.x` when two joined tables share a name.
3. `select` picks and computes columns, `withColumn` / `withColumns` add or replace, `withColumnRenamed` renames, `drop` removes (silently ignores unknown names).
4. `filter` = `where`. Combine with `&`, `|`, `~` and **wrap each comparison in parentheses**.

    | SQL | PySpark |
    |---|---|
    | `WHERE a = 1 AND b > 2` | `.filter((F.col("a") == 1) & (F.col("b") > 2))` |
    | `WHERE city IN ('Pune','Delhi')` | `.filter(F.col("city").isin("Pune", "Delhi"))` |
    | `WHERE marks BETWEEN 80 AND 90` | `.filter(F.col("marks").between(80, 90))` |
    | `WHERE name LIKE 'A%'` | `.filter(F.col("name").like("A%"))` |
    | `SELECT DISTINCT branch` | `.select("branch").distinct()` |
    | `ORDER BY marks DESC` | `.orderBy(F.col("marks").desc())` |

5. `F.lit()` turns a Python constant into a column; `F.expr()` / `selectExpr` accept SQL text.
6. `distinct()` compares whole rows; `dropDuplicates([cols])` keeps an arbitrary row per key.
7. Give an **explicit schema** (StructType or DDL string) for real data.
8. `df.createOrReplaceTempView("name")` + `spark.sql("SELECT …")` gives the same result and the same plan as the DataFrame API.
'''

EXTRA_TITLE = 'Deep dive: columns, types and ANSI mode'
EXTRA_LEAD = 'Small details that cause the most confusing bugs in week 2.'
EXTRA = [
    dict(t='Spark 4 runs in ANSI mode: bad casts fail loudly',
         code='''bad = spark.createDataFrame([("12",), ("abc",)], ["v"])

bad.select(F.col("v").try_cast("int").alias("v_int")).show()
print(spark.conf.get("spark.sql.ansi.enabled"))''',
         expl='''Since Spark 4.0, `spark.sql.ansi.enabled` is `true`: a cast that cannot succeed (`"abc"` → `int`), an integer overflow or a division by zero **raises an error** instead of quietly returning NULL.

When bad values are expected, use **`try_cast`** (or `try_divide`, `try_add`): they return NULL for the bad row and keep going.'''),
    dict(t='The same cast without try_ fails the whole job', error=True,
         code='''bad.select(F.col("v").cast("int")).show()''',
         expl='''One bad string anywhere in a billion rows stops the job. That is the point of ANSI mode: silent NULLs used to hide data-quality problems until a report was wrong.

Read the error class in brackets (`CAST_INVALID_INPUT`) — Spark 4 error messages name the exact cause and suggest the fix.'''),
    dict(t='Spark types you will meet',
         md='''| Spark type | Python value | DDL name | Note |
|---|---|---|---|
| `IntegerType` / `LongType` | `int` | `INT` / `BIGINT` | Python ints are inferred as `long` |
| `DoubleType` | `float` | `DOUBLE` | Use `DecimalType` for money |
| `DecimalType(p, s)` | `decimal.Decimal` | `DECIMAL(10,2)` | Exact arithmetic |
| `StringType` | `str` | `STRING` | |
| `BooleanType` | `bool` | `BOOLEAN` | |
| `DateType` / `TimestampType` | `date` / `datetime` | `DATE` / `TIMESTAMP` | Timestamps follow the session time zone |
| `ArrayType`, `MapType`, `StructType` | `list`, `dict`, `Row` | `ARRAY<…>`, `MAP<…>`, `STRUCT<…>` | Chapter 8 |'''),
    dict(t='Why parentheses matter in filters', error=True,
         code='''students.filter(F.col("branch") == "CSE" & F.col("marks") > 80).show()''',
         expl='''Python evaluates `"CSE" & F.col("marks")` first, because `&` binds tighter than `==` and `>`. What is left, `a == b > 80`, is a Python **chained comparison**, which needs a True/False value from a Column — and PySpark refuses with `CANNOT_CONVERT_COLUMN_INTO_BOOL`.

The fix is always the same: `(F.col("branch") == "CSE") & (F.col("marks") > 80)`.'''),
]

MCQS = [
    dict(q='What does this print?', kind='output', run=True, check="['a', 'b']",
         code='''df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
print(df.drop("c", "zzz").columns)''',
         o=["`['a', 'b']`", 'It raises an error: column zzz does not exist', "`['a', 'b', 'c']`", "`['c']`"], a=0,
         why='`drop` silently ignores names that do not exist, so only `c` is removed.'),
    dict(q='Which filter keeps rows where branch is CSE **and** marks are above 80?', kind='concept',
         o=['`df.filter(F.col("branch") == "CSE" and F.col("marks") > 80)`', '`df.filter((F.col("branch") == "CSE") & (F.col("marks") > 80))`',
            '`df.filter(F.col("branch") == "CSE" & F.col("marks") > 80)`', '`df.filter("branch" == "CSE", "marks" > 80)`'], a=1,
         why='Column conditions are combined with `&`, and each comparison needs its own parentheses because `&` binds tighter than `==`.'),
    dict(q='What does this print?', kind='output', run=True, check='4',
         code='''df = spark.createDataFrame([(1,), (5,), (10,), (11,), (8,)], ["x"])
print(df.filter(F.col("x").between(5, 11)).count())''',
         o=['2', '3', '4', '5'], a=2,
         why='`between` is inclusive at both ends: 5, 10, 11 and 8 qualify.'),
    dict(q='`df.withColumn("marks", F.col("marks") + 5)` — what happens to the existing `marks` column?', kind='concept',
         o=['A second column named marks is added', 'It raises an error: column already exists', 'It is replaced by the new values', 'The original DataFrame df is changed in place'], a=2,
         why='withColumn replaces a column with the same name. It returns a new DataFrame; `df` itself never changes.'),
    dict(q='The code below throws an error. What is the fix?', kind='debug',
         code='''df.withColumn("source", "web")''',
         o=['Use `F.lit("web")` — withColumn needs a Column, not a plain string', 'Use double quotes', 'Use `withColumns`', 'Columns cannot hold text'], a=0,
         why='The second argument must be a Column expression. `F.lit("web")` wraps the constant as a column.'),
    dict(q='What is the difference between `distinct()` and `dropDuplicates(["city"])`?', kind='concept',
         o=['None — they are aliases', 'distinct compares all columns; dropDuplicates(["city"]) keeps one row per city', 'dropDuplicates is an action', 'distinct only works on one column'], a=1,
         why='distinct removes fully identical rows. dropDuplicates with a subset keeps one (arbitrary) full row for each value of those columns.'),
    dict(q='What does this print?', kind='output', run=True, check='DOUBLE',
         code='''df = spark.createDataFrame([(1, 4)], "a INT, b INT")
print(df.select((F.col("a") / F.col("b")).alias("r")).schema["r"].dataType.simpleString().upper())''',
         o=['INT', 'BIGINT', 'DOUBLE', 'DECIMAL'], a=2,
         why='The `/` operator always returns a double in Spark, even for two integers (1 / 4 = 0.25).'),
    dict(q='How do you write `SELECT name, marks * 2 AS doubled FROM df` with the DataFrame API?', kind='concept',
         o=['`df.select("name", "marks * 2 AS doubled")`', '`df.selectExpr("name", "marks * 2 AS doubled")`', '`df.select("name").withColumn("marks * 2")`', '`df.expr("name, marks*2")`'], a=1,
         why='selectExpr parses each string as a SQL expression. In plain `select`, "marks * 2 AS doubled" would be read as a column name.'),
    dict(q='In Spark 4, what does `F.col("v").cast("int")` do when `v` holds `"abc"`?', kind='concept',
         o=['Returns 0', 'Returns NULL', 'Raises a CAST_INVALID_INPUT error (ANSI mode)', 'Returns the string unchanged'], a=2,
         why='ANSI mode is on by default in Spark 4, so invalid casts fail. Use `try_cast` to get NULL instead.'),
    dict(q='Which schema string is valid for `createDataFrame`?', kind='concept',
         o=['`"id: int, name: str"`', '`"id INT, name STRING"`', '`["id INT", "name STRING"]`', '`{"id": int, "name": str}`'], a=1,
         why='A DDL string uses SQL type names separated by commas: `"id INT, name STRING"`.'),
]

PRACTICE = dict(
    kinds=['Rewrite five SQL `SELECT … WHERE … ORDER BY` queries from your SQL notes in PySpark.',
           'Filters with `&`, `|`, `~`, `isin`, `between`, `like` — and NULL-aware conditions.',
           'Derived columns with `withColumn`, `withColumns`, `F.lit` and `cast`.'],
    note='Build each problem\'s example table with `spark.createDataFrame(rows, "col TYPE, …")`, then answer with the DataFrame API only.',
    problems=[
        (1148, 'article-views-i', 'Article Views I', 'Easy', 'Column-to-column comparison, `distinct`, `orderBy`.'),
        (1683, 'invalid-tweets', 'Invalid Tweets', 'Easy', '`F.length` inside a filter.'),
        (620, 'not-boring-movies', 'Not Boring Movies', 'Easy', 'Odd ids with `%`, `!=` on text, sort descending.'),
        (1517, 'find-users-with-valid-e-mails', 'Find Users With Valid E-Mails', 'Easy', '`rlike` with an anchored regular expression.'),
        (1527, 'patients-with-a-condition', 'Patients With a Condition', 'Easy', '`like` / `rlike` on a space-separated list of codes.'),
    ])
