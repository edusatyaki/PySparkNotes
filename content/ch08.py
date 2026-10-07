ORDER = 8
KEY = '8'
TITLE = 'Complex Types: Arrays, Maps, Structs and JSON'
WEEK = 'Week 4'
DATA = ['orders', 'events']

SETUP_TITLE = '8.0 The data — nested, like real JSON'
SETUP = '''from pyspark.sql import functions as F

orders = spark.createDataFrame([
    (1, ("Amit", "Pune", "411001"),   ["phone", "case"],          {"coupon": "NEW10", "channel": "app"}),
    (2, ("Riya", "Mumbai", "400001"), ["laptop"],                 {"channel": "web"}),
    (3, ("Arjun", "Delhi", None),     [],                         {}),
    (4, ("Sneha", "Pune", "411014"),  ["case", "charger", "case"], None),
], "order_id INT, customer STRUCT<name: STRING, city: STRING, pin: STRING>, "
   "items ARRAY<STRING>, meta MAP<STRING, STRING>")

events = spark.createDataFrame([
    (1, '{"user": "u1", "action": "click", "device": {"os": "android", "ver": 14}}'),
    (2, '{"user": "u2", "action": "buy", "amount": 499, "device": {"os": "ios", "ver": 17}}'),
    (3, '{"user": "u1", "action": "view"}'),
], "id INT, payload STRING")'''
SETUP_EXPL = '''Spark columns can hold more than single values:

- **STRUCT** — a fixed set of named fields (`customer.name`), like a nested JSON object.
- **ARRAY** — a list of values of one type (`items`).
- **MAP** — key → value pairs whose keys vary per row (`meta`).

`events.payload` holds raw JSON **text**, as it arrives from Kafka or an API log; this chapter parses it.'''

SECTIONS = [
    dict(t='8.1 Look at nested data', tables=['orders'],
         code='''orders.printSchema()
orders.show(truncate=False)''',
         expl='''`printSchema` draws the nesting as a tree. `show` prints a struct as `{Amit, Pune, 411001}`, an array as `[phone, case]` and a map as `{coupon -> NEW10, …}`.

Note the difference between an **empty** array `[]` (order 3 has no items) and a **NULL** map (order 4 has no metadata at all).'''),

    dict(t='8.2 Read struct fields: dot notation and select("struct.*")', tables=['orders'],
         code='''orders.select("order_id", "customer.name", F.col("customer.city").alias("city")).show()

orders.select("order_id", "customer.*").show()''',
         expl='''`"customer.name"` or `F.col("customer.city")` reach inside a struct. `"customer.*"` flattens **all** its fields into top-level columns — the quickest way to un-nest one level.

Reading one field of a struct in a Parquet file reads only that field from disk (nested column pruning).'''),

    dict(t='8.3 Inspect arrays: size, array_contains, element_at', tables=['orders'],
         code='''orders.select(
    "order_id", "items",
    F.size("items").alias("n_items"),
    F.array_contains("items", "case").alias("has_case"),
    F.try_element_at("items", F.lit(1)).alias("first_item"),
    F.get("items", 0).alias("get_0"),
).show()''',
         expl='''`size` counts elements (0 for an empty array). `array_contains` tests membership. `element_at(col, 1)` takes the first element — **1-based**, negative positions count from the end — while `col[0]` / `getItem(0)` is 0-based.

Order 3 has no items. In Spark 4 (ANSI mode) `element_at` past the end **raises an error**, so this uses `try_element_at`, which returns NULL. For 0-based access, `items[0]` also fails on an empty array; `F.get(col, 0)` returns NULL instead.'''),

    dict(t='8.4 One row per item: explode', tables=['orders'], home=True,
         code='''(orders.select("order_id", F.explode("items").alias("item"))
       .show())''',
         expl='''`explode` turns each array element into its own row and repeats the other columns. Order 1 becomes two rows, order 4 three.

Order 3 **disappears**: an empty (or NULL) array produces no rows. If every order must stay in the result, use `explode_outer` (next slide).'''),

    dict(t='8.5 Keep the empty orders: explode_outer and posexplode', tables=['orders'],
         code='''orders.select("order_id", F.explode_outer("items").alias("item")).show()

orders.select("order_id", F.posexplode("items").alias("pos", "item")) \\
      .filter("order_id = 4").show()''',
         expl='''`explode_outer` keeps rows with an empty or NULL array and gives them a NULL item — the LEFT JOIN of exploding.

`posexplode` also returns each element's position (0-based), useful when order inside the array matters ("the first item added to the cart").'''),

    dict(t='8.6 Clean arrays without exploding', tables=['orders'],
         code='''orders.select(
    "order_id",
    F.array_distinct("items").alias("distinct_items"),
    F.sort_array("items").alias("sorted"),
    F.array_union("items", F.array(F.lit("bag"))).alias("plus_bag"),
    F.array_join("items", ", ").alias("as_text"),
).show(truncate=False)''',
         expl='''Array functions work on each array in place, so there is no shuffle and no change in the row count: `array_distinct`, `sort_array`, `array_union` / `array_intersect` / `array_except`, `array_join`, `slice`, `flatten`, `arrays_zip`.

`F.array(...)` builds an array from columns or literals.'''),

    dict(t='8.7 Transform every element: transform, filter, aggregate', tables=['orders'],
         code='''orders.select(
    "order_id",
    F.transform("items", lambda x: F.upper(x)).alias("upper"),
    F.filter("items", lambda x: x != "case").alias("no_case"),
    F.aggregate("items", F.lit(0), lambda acc, x: acc + F.length(x)).alias("total_chars"),
).show(truncate=False)''',
         expl='''These are **higher-order functions**. The Python `lambda` here is **not** run in Python: it is translated once into a Spark expression and runs in the JVM for every element. So it is as fast as any built-in — very different from a UDF.

`aggregate(array, start, merge)` folds an array into one value, like Python's `functools.reduce`.'''),

    dict(t='8.8 Read a map: keys, values and one key', tables=['orders'],
         code='''orders.select(
    "order_id",
    F.map_keys("meta").alias("keys"),
    F.col("meta")["channel"].alias("channel"),
    F.element_at("meta", "coupon").alias("coupon"),
).show(truncate=False)''',
         expl='''`meta["channel"]` or `element_at(meta, "channel")` looks up one key; a missing key gives NULL. `map_keys` / `map_values` return arrays.

Use a **map** when keys differ from row to row (tags, attributes) and a **struct** when every row has the same fields — structs are typed per field and store more efficiently.'''),

    dict(t='8.9 Explode a map into key/value rows', tables=['orders'],
         code='''orders.select("order_id", F.explode("meta").alias("key", "value")).show()''',
         expl='''Exploding a map returns two columns, key and value — one row per entry. Order 3 (empty map) and order 4 (NULL map) produce no rows, exactly as with arrays.'''),

    dict(t='8.10 Parse JSON text into columns: from_json', tables=['events'],
         code='''schema = "user STRING, action STRING, amount INT, device STRUCT<os: STRING, ver: INT>"

parsed = events.withColumn("e", F.from_json("payload", schema))
parsed.select("id", "e.user", "e.action", "e.amount", "e.device.os").show()''',
         expl='''`from_json(col, schema)` parses a JSON string into a struct using the schema you give — a DDL string works. Fields missing from a document (`amount`, `device` in event 3) become NULL; extra fields in the JSON are ignored.

Always give the schema. `schema_of_json` can suggest one from a sample, but inferring it per row would be slow and unstable.'''),

    dict(t='8.11 Pull a single value without a schema: get_json_object', tables=['events'],
         code='''events.select(
    "id",
    F.get_json_object("payload", "$.action").alias("action"),
    F.get_json_object("payload", "$.device.os").alias("os"),
).show()''',
         expl='''`get_json_object(col, "$.path")` extracts one value by a JSONPath and always returns a **string**. Good for one or two fields; for many fields `from_json` is faster because it parses each document once.'''),

    dict(t='8.12 Build nested output: struct, to_json and collect_list', tables=['orders'],
         code='''flat = orders.select("customer.city", "customer.name", "order_id")

nested = (flat.groupBy("city")
              .agg(F.collect_list(F.struct("order_id", "name")).alias("orders")))
nested.select("city", F.to_json("orders").alias("json")).show(truncate=False)''',
         expl='''`F.struct(...)` packs columns into one struct column; `collect_list` of structs builds an array of objects per group; `to_json` turns any struct, array or map into JSON text.

This is the reverse of `explode` and how you produce nested JSON for an API or a document store.'''),
]

POINTS = '''
1. Three complex types: **STRUCT** (fixed named fields), **ARRAY** (list of one type), **MAP** (varying keys).
2. Struct fields: `"s.field"`, `F.col("s.field")`, or `select("s.*")` to flatten one level.
3. Arrays: `size`, `array_contains`, `element_at` (1-based) vs `getItem` (0-based), `array_distinct`, `sort_array`, `array_join`.
4. `explode` drops rows with empty/NULL arrays; `explode_outer` keeps them; `posexplode` adds the position.
5. Higher-order functions `transform`, `filter`, `aggregate`, `exists` run in the JVM — the lambda is not Python at run time.
6. JSON: `from_json(col, schema)` for many fields, `get_json_object(col, "$.path")` for one, `to_json` to write JSON.
7. Re-nest with `F.struct` + `collect_list` — the inverse of `explode`.

    | Need | Function |
    |---|---|
    | Array → rows | `explode`, `explode_outer`, `posexplode` |
    | Rows → array | `collect_list`, `collect_set` |
    | Text → struct | `from_json` |
    | Struct → text | `to_json` |
'''

EXTRA_TITLE = 'Deep dive: nested data in practice'
EXTRA_LEAD = 'The traps that appear when you flatten real JSON.'
EXTRA = [
    dict(t='Exploding two arrays multiplies the rows',
         code='''t = spark.createDataFrame([(1, ["a", "b"], [10, 20])], "id INT, xs ARRAY<STRING>, ys ARRAY<INT>")

print(t.withColumn("x", F.explode("xs")).withColumn("y", F.explode("ys")).count())

t.select("id", F.explode(F.arrays_zip("xs", "ys")).alias("z")) \\
 .select("id", "z.xs", "z.ys").show()''',
         expl='''Two explodes in a row build a **cross product**: 2 × 2 = 4 rows, pairing every x with every y. When the arrays are parallel (element i of one belongs with element i of the other), `arrays_zip` them first and explode once.'''),
    dict(t='Malformed JSON becomes NULL, not an error',
         code='''bad = spark.createDataFrame([('{"user": "u9", "action": "click"}',), ('{"user": "u10", action}',)], ["payload"])
bad.select(F.from_json("payload", "user STRING, action STRING").alias("e")).show()''',
         expl='''With the default `PERMISSIVE` mode a document that cannot be parsed returns a struct of NULLs instead of failing the job. Count those rows after parsing; silent data loss is the usual result of a schema mismatch.'''),
    dict(t='Flatten every struct automatically',
         code='''def flatten(df):
    from pyspark.sql.types import StructType
    cols = []
    for f in df.schema.fields:
        if isinstance(f.dataType, StructType):
            cols += [F.col(f"{f.name}.{c.name}").alias(f"{f.name}_{c.name}") for c in f.dataType.fields]
        else:
            cols.append(F.col(f.name))
    return df.select(cols)

print(flatten(orders).columns)''',
         expl='''The schema is ordinary Python data (`df.schema.fields`), so you can write helpers that walk it. This one flattens one level of structs with prefixed names; call it again for deeper nesting.'''),
]

MCQS = [
    dict(q='What does this print?', kind='output', run=True, check='3',
         code='''df = spark.createDataFrame([(1, ["a", "b"]), (2, []), (3, ["c"])], "id INT, xs ARRAY<STRING>")
print(df.select(F.explode("xs")).count())''',
         o=['2', '3', '4', '5'], a=1,
         why='explode creates one row per element: 2 + 0 + 1 = 3. The empty array produces no rows.'),
    dict(q='Which function keeps a row whose array is empty, with a NULL element?', kind='concept',
         o=['`explode`', '`posexplode`', '`explode_outer`', '`flatten`'], a=2,
         why='explode_outer emits one row with NULL for an empty or NULL array.'),
    dict(q='What does this print?', kind='output', run=True, check='a',
         code='''df = spark.createDataFrame([(["a", "b", "c"],)], "xs ARRAY<STRING>")
print(df.select(F.element_at("xs", 1)).first()[0])''',
         o=['`a`', '`b`', '`None`', 'It raises an error'], a=0,
         why='element_at is 1-based, so position 1 is the first element. `getItem(1)` would return `b`.'),
    dict(q='Why is `F.transform("items", lambda x: F.upper(x))` fast, unlike a Python UDF?', kind='concept',
         o=['It runs on the GPU', 'The lambda is translated into a Spark expression once and runs in the JVM', 'It caches the result', 'It skips NULL values'], a=1,
         why='Higher-order function lambdas build Column expressions; no Python code runs per element.'),
    dict(q='You need 15 fields from a JSON string column. Which is best?', kind='concept',
         o=['15 calls to `get_json_object`', 'One `from_json` with a schema, then select the fields', 'A Python UDF with `json.loads`', '`split` on commas'], a=1,
         why='from_json parses each document once into a typed struct; get_json_object would parse it 15 times and return only strings.'),
    dict(q='What does this print?', kind='output', run=True, check='4',
         code='''df = spark.createDataFrame([(["a", "b"], [1, 2])], "xs ARRAY<STRING>, ys ARRAY<INT>")
print(df.withColumn("x", F.explode("xs")).withColumn("y", F.explode("ys")).count())''',
         o=['2', '4', '1', '3'], a=1,
         why='Two explodes create a cross product: 2 × 2 = 4 rows. Use arrays_zip to pair elements.'),
    dict(q='This select fails with an error. What is wrong?', kind='debug', run=True, error=True,
         code='''orders.select(F.col("customer").name).show()''',
         o=['Struct fields need `"customer.name"` or `F.col("customer.name")` — `.name` is the Column method `name()`', 'Structs cannot be selected', 'name is a reserved word in Spark SQL', 'show() does not work on structs'], a=0,
         why='`Column.name` is a method (an alias of `alias`), so `F.col("customer").name` is not a field access. Use `F.col("customer.name")` or `F.col("customer")["name"]`.'),
    dict(q='When should you prefer a MAP column over a STRUCT?', kind='concept',
         o=['When every row has the same fields', 'When the set of keys varies from row to row', 'When values have different types', 'Never — maps are deprecated'], a=1,
         why='Maps suit dynamic keys (tags, attributes). Structs are better for a fixed set of typed fields.'),
]

PRACTICE = dict(
    kinds=['Parse a JSON log column with `from_json`, flatten it, and count events per type.',
           'Explode an order\'s items, aggregate per product, then rebuild nested JSON with `collect_list(struct)`.',
           'Clean arrays in place with `transform`, `filter` and `array_distinct` — no explode.'],
    problems=[
        (1484, 'group-sold-products-by-the-date', 'Group Sold Products By The Date', 'Easy', '`collect_set` → `array_sort` → `array_join`.'),
        (2356, 'number-of-unique-subjects-taught-by-each-teacher', 'Number of Unique Subjects Taught by Each Teacher', 'Easy', 'Compare `countDistinct` with `size(collect_set(...))`.'),
    ])
