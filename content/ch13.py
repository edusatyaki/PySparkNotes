ORDER = 13
KEY = '13'
TITLE = 'MLlib: features, pipelines, evaluation and tuning'
WEEK = 'Week 7'
DATA = ['customers (churn)']

SETUP_TITLE = '13.0 The data — will the customer leave?'
SETUP = '''import random
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

rng = random.Random(7)
rows = []
for i in range(1, 201):
    contract = rng.choice(["Monthly", "Monthly", "Yearly", "TwoYear"])
    tenure = rng.randint(1, 60)
    charges = round(rng.uniform(300, 1500), 0)
    calls = rng.randint(0, 6)
    risk = (1.5 if contract == "Monthly" else -1.0) - tenure / 20 + calls / 2 + (charges - 900) / 600
    churned = 1 if risk + rng.gauss(0, 0.8) > 0.5 else 0
    rows.append((i, contract, tenure, float(charges), calls, churned))

customers = spark.createDataFrame(
    rows, "cust_id INT, contract STRING, tenure INT, monthly_charges DOUBLE, support_calls INT, churned INT")
customers.groupBy("churned").count().orderBy("churned").show()'''
SETUP_EXPL = '''200 synthetic telecom customers. Churn is more likely with a monthly contract, short tenure, high charges and many support calls — plus random noise, so no model can be perfect.

The generator uses a fixed seed, so every run of these notes produces exactly the same data. `pyspark.ml` is Spark's DataFrame-based machine-learning library (MLlib); models train in parallel across the cluster.'''

SECTIONS = [
    dict(t='13.1 Estimators and transformers: StringIndexer', tables=['customers'],
         code='''indexer = StringIndexer(inputCol="contract", outputCol="contract_idx")
indexer_model = indexer.fit(customers)          # Estimator.fit → Transformer
print(indexer_model.labels)

indexer_model.transform(customers).select("contract", "contract_idx").distinct().orderBy("contract_idx").show()''',
         expl='''Two kinds of objects run all of MLlib. An **Estimator** learns from data with `fit()` and returns a model. A **Transformer** adds columns with `transform()`.

`StringIndexer` is an estimator: `fit` counts the categories and orders them by frequency (most common → 0.0); the fitted model then maps text to numbers.'''),

    dict(t='13.2 One-hot encode the category', tables=['customers'],
         code='''encoded = OneHotEncoder(inputCols=["contract_idx"], outputCols=["contract_vec"]) \\
    .fit(indexer_model.transform(customers)) \\
    .transform(indexer_model.transform(customers))
encoded.select("contract", "contract_idx", "contract_vec").distinct().orderBy("contract_idx").show()''',
         expl='''An index such as 0, 1, 2 would wrongly tell a linear model that TwoYear is "twice" Yearly. **One-hot encoding** turns it into a vector with a single 1. Spark prints it as a **sparse vector** `(size, [positions], [values])`.

With 3 categories the vector has size **2**: the last category is dropped (`dropLast=True`) because it is implied when the others are 0.'''),

    dict(t='13.3 Pack the features into one vector: VectorAssembler', tables=['customers'],
         code='''assembler = VectorAssembler(
    inputCols=["contract_vec", "tenure", "monthly_charges", "support_calls"],
    outputCol="features")

assembler.transform(encoded).select("features", "churned").show(4, truncate=False)''',
         expl='''Every MLlib model expects **one column of type vector** holding all features, plus a label column. `VectorAssembler` concatenates numeric columns and vectors into that column — here 2 + 1 + 1 + 1 = 5 numbers per customer.'''),

    dict(t='13.4 The whole recipe as a Pipeline', tables=['customers'], home=True,
         code='''lr = LogisticRegression(featuresCol="scaled", labelCol="churned")

pipeline = Pipeline(stages=[
    StringIndexer(inputCol="contract", outputCol="contract_idx"),
    OneHotEncoder(inputCols=["contract_idx"], outputCols=["contract_vec"]),
    VectorAssembler(inputCols=["contract_vec", "tenure", "monthly_charges", "support_calls"],
                    outputCol="features"),
    StandardScaler(inputCol="features", outputCol="scaled"),
    lr,
])
train, test = customers.randomSplit([0.75, 0.25], seed=42)
print(train.count(), test.count())''',
         expl='''A **Pipeline** chains the stages; `pipeline.fit(train)` fits each estimator in order on the training data only and returns a `PipelineModel` that replays every step on new data.

That is what prevents **data leakage**: the scaler's mean and standard deviation, and the indexer's categories, are learned from the training set alone. `randomSplit` with a seed gives a reproducible 75/25 split.'''),

    dict(t='13.5 Fit and predict', tables=['customers'],
         code='''from pyspark.ml.functions import vector_to_array

model = pipeline.fit(train)
pred = model.transform(test)

(pred.select("cust_id", "contract", "tenure", "support_calls", "churned", "prediction",
             F.round(vector_to_array("probability")[1], 3).alias("p_churn"))
     .orderBy("cust_id").show(8))''',
         expl='''`pipeline.fit(train)` runs every stage in order and returns a `PipelineModel`; `transform(test)` adds `rawPrediction`, `probability` (a vector: [P(stay), P(churn)]) and `prediction` (0 or 1, using a 0.5 threshold).

`vector_to_array` turns the probability vector into an array so element 1, the churn probability, can be selected like any column.'''),

    dict(t='13.6 How good is it? AUC and accuracy', tables=['customers'],
         code='''auc = BinaryClassificationEvaluator(labelCol="churned", metricName="areaUnderROC").evaluate(pred)
acc = MulticlassClassificationEvaluator(labelCol="churned", metricName="accuracy").evaluate(pred)
print(f"AUC      = {auc:.3f}")
print(f"accuracy = {acc:.3f}")

pred.groupBy("churned").pivot("prediction", [0.0, 1.0]).count().orderBy("churned").show()''',
         expl='''**AUC** (area under the ROC curve) measures how well the predicted probabilities rank churners above non-churners: 0.5 is a coin toss, 1.0 is perfect. **Accuracy** is the share of correct 0/1 predictions.

The pivot is a **confusion matrix**: rows are the true label, columns the prediction. Off-diagonal cells are the mistakes — false alarms and missed churners.'''),

    dict(t='13.7 What did the model learn? Coefficients', tables=['customers'],
         code='''lr_model = model.stages[-1]
names = ["contract=" + l for l in model.stages[0].labels[:-1]] + ["tenure", "monthly_charges", "support_calls"]
for n, c in zip(names, lr_model.coefficients):
    print(f"{n:20} {c:+.3f}")
print(f"{'intercept':20} {lr_model.intercept:+.3f}")''',
         expl='''`model.stages` holds the fitted stages; the last is the trained `LogisticRegressionModel`. Because the features were standardised, the coefficient sizes are comparable: positive = raises churn risk.

The signs match how the data was generated — short tenure (negative tenure coefficient), more support calls and a monthly contract push customers to leave.'''),

    dict(t='13.8 A different model: random forest and feature importances', tables=['customers'],
         code='''rf_pipeline = Pipeline(stages=pipeline.getStages()[:-2] + [
    RandomForestClassifier(featuresCol="features", labelCol="churned", numTrees=50, maxDepth=4, seed=42)])
rf_model = rf_pipeline.fit(train)
rf_pred = rf_model.transform(test)

print(f"RF AUC = {BinaryClassificationEvaluator(labelCol='churned').evaluate(rf_pred):.3f}")
for n, imp in zip(names, rf_model.stages[-1].featureImportances.toArray()):
    print(f"{n:20} {imp:.3f}")''',
         expl='''Swapping the model means swapping one stage — the feature stages are reused. Trees need no scaling, so the scaler is left out. `featureImportances` shows how much each feature contributed to the forest's splits.

`seed=42` makes the forest reproducible; without it every run builds different trees.'''),

    dict(t='13.9 Tune hyper-parameters: CrossValidator', tables=['customers'],
         code='''from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

grid = (ParamGridBuilder()
        .addGrid(lr.regParam, [0.0, 0.1, 1.0])
        .addGrid(lr.elasticNetParam, [0.0, 1.0])
        .build())
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid,
                    evaluator=BinaryClassificationEvaluator(labelCol="churned"),
                    numFolds=3, seed=42, parallelism=2)
cv_model = cv.fit(train)

for params, metric in zip(grid, cv_model.avgMetrics):
    print({p.name: v for p, v in params.items()}, round(metric, 3))''',
         expl='''`ParamGridBuilder` lists the combinations to try (3 × 2 = 6). `CrossValidator` fits every combination on 3 train/validation folds — 18 fits — and keeps the best by average AUC; `cv_model.bestModel` is refitted on all the training data.

Read the last line: strong L1 regularisation (`regParam=1.0`, `elasticNetParam=1.0`) pushed every coefficient to zero, so the model predicts the same probability for everyone — AUC 0.5, a coin toss. `parallelism=2` trains two candidate models at the same time.'''),

    dict(t='13.10 Save the model and score new customers', tables=['customers'],
         code='''model.write().overwrite().save("models/churn_lr")
loaded = PipelineModel.load("models/churn_lr")

new = spark.createDataFrame([(901, "Monthly", 2, 1400.0, 5), (902, "TwoYear", 48, 500.0, 0)],
                            "cust_id INT, contract STRING, tenure INT, monthly_charges DOUBLE, support_calls INT")
loaded.transform(new).select("cust_id", "contract", "tenure", "prediction").show()''',
         expl='''A `PipelineModel` saves every fitted stage (indexer labels, scaler statistics, coefficients) to a folder; `PipelineModel.load` restores it in any other Spark job. New raw data goes in, predictions come out — no feature code to rewrite.

The new-customer DataFrame has **no `churned` column**: only the input columns the stages need.'''),

    dict(t='13.11 Without labels: KMeans clustering', tables=['customers'],
         code='''from pyspark.ml.clustering import KMeans

feats = VectorAssembler(inputCols=["tenure", "monthly_charges"], outputCol="f").transform(customers)
scaled = StandardScaler(inputCol="f", outputCol="features").fit(feats).transform(feats)
km = KMeans(k=3, seed=1).fit(scaled)

(km.transform(scaled).groupBy("prediction")
   .agg(F.count("*").alias("customers"), F.round(F.avg("tenure"), 1).alias("avg_tenure"),
        F.round(F.avg("monthly_charges"), 0).alias("avg_charges"))
   .orderBy("prediction").show())''',
         expl='''**KMeans** groups customers into k clusters by similarity, with no label. Scaling first matters: otherwise `monthly_charges` (hundreds) would dominate `tenure` (tens).

Describe each cluster with ordinary aggregations to turn cluster numbers into segments a business can use.'''),
]

POINTS = '''
1. **Estimator** `.fit(df)` → **Model** (a Transformer) `.transform(df)` adds columns.
2. Features must be **one vector column**: `StringIndexer` → `OneHotEncoder` → `VectorAssembler` (→ `StandardScaler` for linear models).
3. Put every step in a **Pipeline**; fit it on the training data only to avoid leakage. Save/load the whole `PipelineModel`.
4. `randomSplit([0.75, 0.25], seed=…)` for a reproducible split; seed tree models too.
5. Evaluate with `BinaryClassificationEvaluator` (AUC), `MulticlassClassificationEvaluator` (accuracy, F1), `RegressionEvaluator` (RMSE); build a confusion matrix with a pivot.
6. Tune with `ParamGridBuilder` + `CrossValidator` (or the cheaper `TrainValidationSplit`).

    | Task | Common MLlib classes |
    |---|---|
    | Classification | `LogisticRegression`, `RandomForestClassifier`, `GBTClassifier` |
    | Regression | `LinearRegression`, `RandomForestRegressor`, `GBTRegressor` |
    | Clustering | `KMeans`, `BisectingKMeans` |
    | Recommendation | `ALS` |
'''

EXTRA_TITLE = 'Deep dive: MLlib in a real project'
EXTRA_LEAD = 'When to use MLlib, and when to aggregate in Spark and train elsewhere.'
EXTRA = [
    dict(t='MLlib, or Spark for features and scikit-learn for the model?',
         md='''| Situation | Good choice |
|---|---|
| Training data does not fit on one machine | **MLlib** (distributed training) |
| Training data fits in memory after aggregation | Spark for features → `toPandas()` → scikit-learn / XGBoost |
| Many small models (one per store / product) | `groupBy().applyInPandas()` with scikit-learn inside (Chapter 11) |
| Scoring a huge table with a model trained elsewhere | `mapInPandas` / pandas UDF that loads the model once per partition |
| Deep learning | Dedicated frameworks (PyTorch, TensorFlow), with Spark for data preparation |''',
         expl='''In practice, Spark's biggest role in ML is **feature engineering at scale**: the joins, windows and aggregations of Chapters 4–7 that turn billions of events into one row per customer.'''),
    dict(t='Data leakage: fit transformers on the training set only',
         md='''| Wrong | Right |
|---|---|
| `scaler.fit(all_data)` then split | split, then `pipeline.fit(train)` |
| Encode categories seen only in the test set | `StringIndexer(handleInvalid="keep")` for unseen values at scoring time |
| Features computed with future information (e.g. "total orders" including after the churn date) | Compute features only from data before the prediction date |''',
         expl='''Leakage makes offline metrics look excellent and production results disappointing. A Pipeline fitted on the training split prevents the first two mistakes; the third is a modelling discipline — window functions with a time cut-off help.'''),
]

MCQS = [
    dict(q='In MLlib, what does calling `.fit(df)` on an Estimator return?', kind='concept',
         o=['A DataFrame with predictions', 'A Model, which is a Transformer', 'Nothing — it trains in place', 'A Pipeline'], a=1,
         why='Estimators learn from data and return a fitted Model; the model\'s transform() adds prediction columns.'),
    dict(q='Why does a model need `VectorAssembler`?', kind='concept',
         o=['To remove NULLs', 'MLlib models expect all features in a single vector column', 'To one-hot encode text', 'To split train and test'], a=1,
         why='Every MLlib algorithm reads one features column of vector type.'),
    dict(q='A `StandardScaler` was fitted on the full dataset before splitting into train and test. What is the problem?', kind='concept',
         o=['Nothing', 'Data leakage: test-set statistics influenced training', 'The scaler only works on vectors', 'Scaling breaks logistic regression'], a=1,
         why='Fit every transformer on the training data only — a Pipeline fitted on train does this automatically.'),
    dict(q='What does this print?', kind='output', run=True, check='2',
         code='''df = spark.createDataFrame([("a",), ("b",), ("c",)], ["cat"])
idx = StringIndexer(inputCol="cat", outputCol="i").fit(df).transform(df)
v = OneHotEncoder(inputCols=["i"], outputCols=["v"]).fit(idx).transform(idx)
print(v.first().v.size)''',
         o=['1', '2', '3', '4'], a=1,
         why='With 3 categories and dropLast=True (the default), the one-hot vector has 3 − 1 = 2 positions.'),
    dict(q='A CrossValidator has a grid of 4 parameter combinations and numFolds=5. How many models does it train before refitting the best one?', kind='concept',
         o=['4', '5', '9', '20'], a=3,
         why='Each of the 4 combinations is trained on each of the 5 folds: 20 fits, then one final fit of the best.'),
    dict(q='Scoring fails on new data with an "unseen label" error from StringIndexer. What is the fix?', kind='debug',
         code='''StringIndexer(inputCol="city", outputCol="city_idx")''',
         o=['Drop the city column', 'Set `handleInvalid="keep"` so unseen categories get their own index', 'Use a larger dataset', 'Call fit on the new data'], a=1,
         why='handleInvalid="keep" maps categories not seen during fit to an extra index instead of failing.'),
    dict(q='Which metric is 0.5 for random guessing and 1.0 for a perfect ranking of positives above negatives?', kind='concept',
         o=['Accuracy', 'RMSE', 'Area under the ROC curve (AUC)', 'Log loss'], a=2,
         why='AUC measures ranking quality; random scores give 0.5.'),
]

PRACTICE = dict(
    kinds=['Rebuild the churn pipeline with GBTClassifier and compare AUC on the same split.',
           'Add a derived feature (charges per month of tenure) and check whether AUC improves.',
           'Build features from raw events with windows and aggregations, then train — the realistic workflow.'],
    note='For practice datasets, the Telco Customer Churn and the Bank Marketing datasets are commonly used public examples; load them with an explicit schema.')
