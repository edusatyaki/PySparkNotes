ORDER = 3
KEY = '3'
TITLE = 'Built-in Functions: Strings, Numbers, Dates, CASE WHEN and NULLs'
WEEK = 'Week 2'
DATA = ['employees']

SETUP_TITLE = '3.0 The data — deliberately messy'
SETUP = '''from pyspark.sql import functions as F

employees = spark.createDataFrame([
    (101, "  ravi KUMAR ", "ravi.kumar@infosys.com", "Engineering", 85250.4567, "2021-03-15", "9876543210", None),
    (102, "Sara Khan",     "sara@tcs.co.in",         "Sales",       62000.0,    "2022-11-01", None,         101),
    (103, "arjun mehta",   "arjun.m@wipro.com",      "Engineering", 91000.555,  "2019-07-22", "9123456780", 101),
    (104, "Priya Nair",    "priya.nair@infosys.com", "HR",          None,       "2023-01-09", "98765",      102),
    (105, "DEV  sharma",   "dev@zoho.in",            None,          54000.5,    "2020-05-30", "9988776655", 102),
    (106, "Neha Gupta",    "neha.g@tcs.co.in",       "Sales",       71000.0,    "2024-02-29", None,         None),
], "emp_id INT, name STRING, email STRING, dept STRING, salary DOUBLE, "
   "join_date STRING, phone STRING, manager_id INT")'''
SETUP_EXPL = '''Six employees with the problems real data has: names with stray spaces and random capitals, salaries with too many decimals, dates stored as **text**, a 5-digit phone number, and NULLs in `dept`, `salary`, `phone` and `manager_id`.

Every function in this chapter comes from `pyspark.sql.functions` (imported as `F`) and runs inside the JVM — no Python per row.'''

SECTIONS = [
    dict(divider=True, t='Strings'),
    dict(t='3.1 Fix the messy names: trim, lower, initcap', tables=['employees'], home=True,
         code='''employees.select(
    "name",
    F.initcap(F.lower(F.trim("name"))).alias("clean"),
    F.upper("name").alias("upper"),
).show(truncate=False)''',
         expl='''`trim` removes spaces at both ends (`ltrim` / `rtrim` for one side). `lower` then `initcap` gives "Ravi Kumar" whatever the original capitals were.

The inner double space in "DEV  sharma" survives — `trim` only cleans the ends. The next slide fixes that with a regular expression.'''),

    dict(t='3.2 Collapse inner spaces and measure length: regexp_replace, length', tables=['employees'],
         code='''tidy = F.initcap(F.lower(F.trim(F.regexp_replace("name", r"\\s+", " "))))

employees.select(tidy.alias("name"),
                 F.length("name").alias("raw_len"),
                 F.length(tidy).alias("clean_len")).show()''',
         expl='''`regexp_replace(col, pattern, replacement)` uses Java regular expressions: `\\s+` = one or more whitespace characters, replaced by a single space.

A Column expression can be stored in a Python variable (`tidy`) and reused — it is just a description of the computation until a query runs it.'''),

    dict(t='3.3 Build and split text: concat_ws, split, substring', tables=['employees'],
         code='''employees.select(
    F.concat_ws(" | ", "emp_id", "dept").alias("label"),
    F.split("email", "@").getItem(1).alias("domain"),
    F.substring("join_date", 1, 4).alias("year_text"),
).show(truncate=False)''',
         expl='''`concat_ws(sep, …)` joins columns with a separator and **skips NULLs** (employee 105 shows just `105`). Plain `concat` returns NULL if any piece is NULL.

`split` returns an **array**; `getItem(1)` takes the second element. `substring(col, pos, len)` counts from **1**, like SQL.'''),

    dict(t='3.4 Extract and mask with regular expressions', tables=['employees'],
         code='''employees.select(
    "email",
    F.regexp_extract("email", r"@([a-z]+)\\.", 1).alias("company"),
    F.regexp_replace("phone", r"\\d(?=\\d{4})", "*").alias("masked_phone"),
).show(truncate=False)''',
         expl='''`regexp_extract(col, pattern, group)` returns one capture group — here the word after `@`. If there is no match it returns an empty string, not NULL.

The mask pattern `\\d(?=\\d{4})` means "a digit that still has 4 digits after it", so every digit except the last four becomes `*`. NULL phones stay NULL.'''),

    dict(t='3.5 Pad and validate: lpad, rpad and a length check', tables=['employees'],
         code='''employees.select(
    F.lpad(F.col("emp_id").cast("string"), 6, "0").alias("emp_code"),
    "phone",
    (F.length("phone") == 10).alias("valid_phone"),
).show()''',
         expl='''`lpad(col, width, pad)` pads on the left to a fixed width — handy for codes like `000101`. Note that `lpad` also **cuts** values longer than the width.

The check returns `false` for the 5-digit number and **NULL** (not false) for a NULL phone: any comparison with NULL is NULL.'''),

    dict(divider=True, t='Numbers'),
    dict(t='3.6 Round the salaries: round, bround, floor, ceil', tables=['employees'],
         code='''employees.select(
    "salary",
    F.round("salary", 2).alias("round_2"),
    F.round("salary", -3).alias("nearest_1000"),
    F.floor("salary").alias("floor"),
    F.ceil("salary").alias("ceil"),
).show()''',
         expl='''`round(col, scale)` rounds half **up**; a negative scale rounds to tens, hundreds, thousands. `floor` always goes down and `ceil` always up; both return integers (`long`).

`bround` rounds half to **even** (banker's rounding): `bround(2.5)` = 2, `bround(3.5)` = 4. It avoids an upward bias when you round millions of values.'''),

    dict(t='3.7 Distance, parity and limits: abs, %, greatest, least', tables=['employees'],
         code='''employees.select(
    "emp_id", "salary",
    F.round(F.abs(F.col("salary") - 70000)).alias("gap_from_70k"),
    (F.col("emp_id") % 2 == 1).alias("odd_id"),
    F.greatest("salary", F.lit(60000.0)).alias("at_least_60k"),
    F.least("salary", F.lit(80000.0)).alias("capped_80k"),
).show()''',
         expl='''`abs` gives the distance from a target, `%` the remainder (here: is the ID odd?). `greatest` and `least` compare values **across columns in one row** — a floor and a cap on salary — unlike `max`/`min`, which aggregate down a column.

`greatest` and `least` skip NULLs: for the NULL salary they return the other argument, 60000 and 80000. Arithmetic with NULL (`abs`, `%`) stays NULL.'''),

    dict(divider=True, t='Dates and times'),
    dict(t='3.8 Text to real dates: to_date', tables=['employees'],
         code='''dated = employees.withColumn("join_date", F.to_date("join_date", "yyyy-MM-dd"))
dated.select("emp_id", "join_date").printSchema()
dated.select("emp_id", "join_date").show(3)''',
         expl='''`to_date(col, format)` parses text into a `DateType`. The pattern letters are Java's: `yyyy` year, `MM` month, `dd` day, `HH` hour, `mm` **minute**. Mixing up `MM` and `mm` is the classic bug.

From now on the rest of the chapter uses `dated`, where `join_date` is a real date.'''),

    dict(t='3.9 Pull pieces out of a date', tables=['dated'],
         code='''dated.select(
    "join_date",
    F.year("join_date").alias("year"),
    F.month("join_date").alias("month"),
    F.dayofmonth("join_date").alias("day"),
    F.date_format("join_date", "EEEE").alias("weekday"),
    F.quarter("join_date").alias("q"),
).show()''',
         expl='''`year`, `month`, `dayofmonth`, `quarter`, `dayofweek` (1 = Sunday) and `weekofyear` return integers. `date_format` turns a date into **text** with any pattern — `EEEE` is the full weekday name, `MMM` a short month name.

2024-02-29 is valid: 2024 was a leap year.'''),

    dict(t='3.10 Date arithmetic: datediff, months_between, add_months', tables=['dated'],
         code='''ref = F.lit("2026-10-06").cast("date")

dated.select(
    "emp_id", "join_date",
    F.datediff(ref, "join_date").alias("days_in_company"),
    F.floor(F.months_between(ref, "join_date") / 12).alias("years"),
    F.date_add("join_date", 90).alias("probation_end"),
    F.add_months("join_date", 12).alias("first_review"),
).show()''',
         expl='''`datediff(end, start)` counts days; `months_between` returns fractional months, so whole years = `floor(months / 12)`. `date_add` adds days and `add_months` adds calendar months — adding 12 months to 2024-02-29 gives 2025-02-28 because the result is clamped to the last valid day.

A fixed reference date keeps the notes reproducible; in real code use `F.current_date()`.'''),

    dict(t='3.11 Bucket rows by month: date_trunc and last_day', tables=['dated'],
         code='''dated.select(
    "join_date",
    F.date_trunc("month", "join_date").alias("month_start"),
    F.trunc("join_date", "year").alias("year_start"),
    F.last_day("join_date").alias("month_end"),
).show()''',
         expl='''`date_trunc(unit, col)` rounds down to the start of a unit and returns a **timestamp**; `trunc(col, unit)` does the same but returns a **date** (note the argument order is swapped). `last_day` gives the last day of the month.

Truncating is how you group daily data by month or year.'''),

    dict(divider=True, t='CASE WHEN and NULLs'),
    dict(t='3.12 Give each salary a band: when / otherwise', tables=['employees'],
         code='''employees.select(
    "emp_id", "salary",
    F.when(F.col("salary") >= 80000, "High")
     .when(F.col("salary") >= 60000, "Medium")
     .when(F.col("salary").isNotNull(), "Low")
     .otherwise("Unknown").alias("band"),
).show()''',
         expl='''`F.when(cond, value).when(…).otherwise(default)` is SQL `CASE WHEN … ELSE … END`. Conditions are checked **top to bottom** and the first true one wins, so order the bands from strictest to loosest.

Without `.otherwise`, rows that match nothing get NULL. Here the NULL salary falls through to `"Unknown"`.'''),

    dict(t='3.13 Find the missing values: isNull, isNotNull', tables=['employees'],
         code='''employees.filter(F.col("phone").isNull()).select("emp_id", "phone").show()

print(employees.filter(F.col("phone") == None).count())''',
         expl='''`isNull()` / `isNotNull()` are the correct tests. Comparing with `== None` produces `phone = NULL`, which is NULL for every row, so the second filter keeps **nothing** — the count is 0.

This is the same three-valued logic as SQL: NULL means unknown, and unknown is never equal to anything.'''),

    dict(t='3.14 Count NULLs in every column at once', tables=['employees'],
         code='''employees.select([
    F.count(F.when(F.col(c).isNull(), 1)).alias(c)
    for c in employees.columns
]).show()''',
         expl='''A Python list comprehension builds one expression per column; `count` ignores NULLs, so `count(when(isNull, 1))` counts exactly the missing values.

This one-liner is the first thing to run on any new dataset.'''),

    dict(t='3.15 Fill the gaps: fillna, coalesce', tables=['employees'],
         code='''employees.fillna({"dept": "Unassigned", "salary": 0}) \\
         .select("emp_id", "dept", "salary").show()

employees.select("emp_id",
    F.coalesce("phone", F.lit("no phone")).alias("contact")).show()''',
         expl='''`fillna(dict)` replaces NULLs per column; the value type must match the column type (a number for `salary`, text for `dept`).

`coalesce(a, b, …)` returns the first non-NULL argument — use it inside expressions, e.g. a fallback from a second column.'''),

    dict(t='3.16 Drop incomplete rows: dropna', tables=['employees'],
         code='''print(employees.dropna().count())                       # any NULL anywhere
print(employees.dropna(subset=["dept", "salary"]).count())
print(employees.dropna(thresh=7).count())              # at least 7 non-NULL values''',
         expl='''`dropna()` with no arguments removes a row if **any** column is NULL — usually far too aggressive (only 1 of 6 rows survives). Limit it with `subset=[…]` or require a minimum number of filled columns with `thresh=`.'''),

    dict(t='3.17 Turn junk into NULL: nullif and NULL-safe equality', tables=['employees'],
         code='''employees.select(
    "emp_id", "phone",
    F.nullif("phone", F.lit("98765")).alias("phone_fixed"),
    F.col("manager_id").eqNullSafe(F.lit(None)).alias("no_manager"),
).show()''',
         expl='''`nullif(a, b)` returns NULL when `a = b`, otherwise `a` — the standard way to turn a known junk value into NULL.

`eqNullSafe` is SQL's `<=>`: it treats two NULLs as equal and never returns NULL, so it gives true/false for every row.'''),
]

POINTS = '''
1. All built-ins live in `pyspark.sql.functions` (`F`) and run in the JVM. Look there before writing a UDF.
2. Strings: `trim`, `lower`, `upper`, `initcap`, `length`, `concat_ws` (skips NULLs), `split(...).getItem(i)`, `substring` (1-based), `lpad`, `regexp_replace`, `regexp_extract`.
3. Numbers: `round` (half up), `bround` (half even), `floor`, `ceil`; Spark 4 ANSI mode makes division by zero an error — use `try_divide`.
4. Dates: parse with `to_date(col, "yyyy-MM-dd")` (`MM` = month, `mm` = minute).

    | Need | Function |
    |---|---|
    | Part of a date | `year`, `month`, `dayofmonth`, `quarter`, `dayofweek` |
    | Difference | `datediff(end, start)`, `months_between` |
    | Shift | `date_add`, `date_sub`, `add_months` |
    | Bucket | `date_trunc("month", c)` (timestamp), `trunc(c, "month")` (date) |
    | Format as text | `date_format(c, "dd MMM yyyy")` |

5. `F.when(...).when(...).otherwise(...)` = `CASE WHEN`; first true branch wins; no `otherwise` → NULL.
6. Test NULL with `isNull()` / `isNotNull()`, never `== None`.
7. `fillna` / `coalesce` fill, `dropna(subset=, thresh=)` drops, `nullif` creates NULLs, `eqNullSafe` compares NULLs safely.
'''

EXTRA_TITLE = 'Deep dive: functions that surprise people'
EXTRA_LEAD = 'Three behaviours that look like bugs until you know the rule.'
EXTRA = [
    dict(t='Division by zero: error in ANSI mode, NULL with try_divide', error=True,
         code='''nums = spark.createDataFrame([(10, 2), (5, 0)], "a INT, b INT")
nums.select(F.try_divide("a", "b").alias("safe")).show()
nums.select((F.col("a") / F.col("b")).alias("plain")).show()''',
         expl='''`try_divide` returns NULL for the row with `b = 0` and the query succeeds. Plain `/` raises `DIVIDE_BY_ZERO` because Spark 4 runs in ANSI mode.

Older tutorials say "division by zero returns NULL in Spark" — that was the pre-4.0 default.'''),
    dict(t='Date patterns: MM is month, mm is minute',
         code='''raw = spark.createDataFrame([("2024-03-15",)], ["d"])
raw.select(F.to_date("d", "yyyy-MM-dd").alias("right"),
           F.date_format(F.to_date("d", "yyyy-MM-dd"), "dd/MM/yyyy").alias("as_text"),
           F.date_format(F.to_date("d", "yyyy-MM-dd"), "dd/mm/yyyy").alias("wrong")).show()''',
         expl='''With `mm` the formatter prints **minutes** — `00`, because a date has no time part. Nothing fails; the report is silently wrong. Always use capital `MM` for months.'''),
    dict(t='Why NULL > 0 is not false',
         code='''t = spark.createDataFrame([(5,), (None,), (-1,)], "x INT")
t.select("x", (F.col("x") > 0).alias("x>0"),
         F.when(F.col("x") > 0, "pos").otherwise("not pos").alias("label")).show()
print(t.filter(F.col("x") > 0).count(), t.filter(~(F.col("x") > 0)).count())''',
         expl='''For the NULL row `x > 0` is NULL, so it lands in **neither** the filter nor its negation: 1 + 1 rows out of 3. In `when`, NULL is treated like false and falls to `otherwise`.

When a filter and its opposite do not add up to the total row count, a NULL is hiding in the condition.'''),
]

MCQS = [
    dict(q='What does this print?', kind='output', run=True, check='a-c',
         code='''df = spark.createDataFrame([("a", None, "c")], "x STRING, y STRING, z STRING")
print(df.select(F.concat_ws("-", "x", "y", "z")).first()[0])''',
         o=['`None`', '`a--c`', '`a-c`', '`a-None-c`'], a=2,
         why='`concat_ws` skips NULL values entirely, so only `a` and `c` are joined. `concat` would have returned NULL.'),
    dict(q='Which pattern parses the text `"07/03/2025"` as 7 March 2025?', kind='concept',
         o=['`"mm/dd/yyyy"`', '`"dd/MM/yyyy"`', '`"dd/mm/yyyy"`', '`"MM/dd/yyyy"`'], a=1,
         why='Day first, then month: `dd/MM/yyyy`. Lower-case `mm` means minutes.'),
    dict(q='What does this print?', kind='output', run=True, check='2',
         code='''df = spark.createDataFrame([(2.5,)], ["v"])
print(int(df.select(F.bround("v", 0)).first()[0]))''',
         o=['2', '3', '2.5', 'It raises an error'], a=0,
         why='`bround` uses banker\'s rounding (half to even): 2.5 → 2. `round` would give 3.'),
    dict(q='The filter below returns 0 rows although some phones are missing. Why?', kind='debug',
         code='''print(employees.filter(F.col("phone") == None).count())''',
         o=['`== None` compares with NULL, which is never true; use `isNull()`', 'Phones are strings, so you must write `== "None"`', 'filter cannot take None', 'count() ignores NULL rows'], a=0,
         why='Any comparison with NULL yields NULL, and filter keeps only true rows. Use `F.col("phone").isNull()`.'),
    dict(q='A `when` chain has no `.otherwise()`. What do rows that match no condition get?', kind='concept',
         o=['An empty string', 'The value of the last `when`', 'NULL', 'An error is raised'], a=2,
         why='Like SQL CASE without ELSE, unmatched rows get NULL.'),
    dict(q='What does this print?', kind='output', run=True, check='2025-02-28',
         code='''df = spark.createDataFrame([("2024-02-29",)], ["d"])
print(df.select(F.add_months(F.to_date("d"), 12)).first()[0])''',
         o=['2025-02-29', '2025-03-01', '2025-02-28', 'None'], a=2,
         why='add_months keeps the day where possible and clamps to the last day of the target month; February 2025 ends on the 28th.'),
    dict(q='`F.split("email", "@").getItem(1)` on `"sara@tcs.co.in"` returns:', kind='concept',
         o=['`sara`', '`tcs.co.in`', '`["sara", "tcs.co.in"]`', '`tcs`'], a=1,
         why='split returns an array `["sara", "tcs.co.in"]`; getItem is 0-based, so item 1 is the domain.'),
    dict(q='How many rows survive `dropna()` on a DataFrame where every row has at least one NULL?', kind='concept',
         o=['All of them', 'None', 'Half of them', 'It depends on the number of partitions'], a=1,
         why='Default `how="any"` drops a row if any column is NULL. Use `subset` or `thresh` to be less strict.'),
    dict(q='The code below fails in Spark 4. What is the most likely fix?', kind='debug',
         code='''df.select(F.col("revenue") / F.col("orders"))''',
         o=['Cast both columns to string', 'Use `F.try_divide("revenue", "orders")` because some orders are 0', 'Use `//` instead of `/`', 'Wrap it in `F.lit`'], a=1,
         why='ANSI mode raises DIVIDE_BY_ZERO; `try_divide` returns NULL for those rows instead.'),
    dict(q='Which function returns the **first non-NULL** value from several columns?', kind='concept',
         o=['`F.nullif`', '`F.coalesce`', '`F.first`', '`F.nvl2`'], a=1,
         why='coalesce(a, b, c) returns a if not NULL, else b, else c.'),
]

PRACTICE = dict(
    kinds=['Clean a text column end to end: trim, collapse spaces, fix case, validate length.',
           'Parse text dates, then compute age in days and years, and bucket by month.',
           'Profile a new dataset: NULL count per column, then decide fill vs drop per column.'],
    problems=[
        (1667, 'fix-names-in-a-table', 'Fix Names in a Table', 'Easy', '`upper` + `lower` + `substring` (or `initcap`).'),
        (1484, 'group-sold-products-by-the-date', 'Group Sold Products By The Date', 'Easy', '`collect_set`, `array_sort`, `concat_ws`.'),
        (1873, 'calculate-special-bonus', 'Calculate Special Bonus', 'Easy', '`when` with `%` and `startswith`.'),
        (1141, 'user-activity-for-the-past-30-days-i', 'User Activity for the Past 30 Days I', 'Easy', '`date_sub` / `datediff` in a filter, then count distinct.'),
        (197, 'rising-temperature', 'Rising Temperature', 'Easy', '`date_add` in a self-join condition (or `lag`).'),
    ])
