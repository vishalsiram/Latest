import re
import pickle
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Load the saved models and vectorizers
clf_cat1 = pickle.load(open("clf_cat1.pkl", "rb"))
clf_cat2 = pickle.load(open("clf_cat2.pkl", "rb"))
tfidf_payer_name = pickle.load(open("tfidf_payer_name.pkl", "rb"))
tfidf_payee_name = pickle.load(open("tfidf_payee_name.pkl", "rb"))
tfidf_payee_account_type = pickle.load(open("tfidf_payee_account_type.pkl", "rb"))
tfidf_payer_account_type = pickle.load(open("tfidf_payer_account_type.pkl", "rb"))
tfidf_payer_vpa = pickle.load(open("tfidf_payer_vpa.pkl", "rb"))
tfidf_payee_vpa = pickle.load(open("tfidf_payee_vpa.pkl", "rb"))

# Assuming df2 is your input DataFrame
# Check for None values and convert to lowercase
for col_name in ["payer_name", "payee_name", "payee_account_type", "payer_account_type", "payer_vpa", "payee_vpa"]:
    df2 = df2.withColumn(col_name, col(col_name).cast("string").lower())

# Define a function to preprocess and predict the categories
def preprocess_and_predict(row):
    # Transform input data using the TFIDF vectorizers
    payer_name_vec = tfidf_payer_name.transform([row.payer_name])
    payee_name_vec = tfidf_payee_name.transform([row.payee_name])
    payee_account_type_vec = tfidf_payee_account_type.transform([row.payee_account_type])
    payer_account_type_vec = tfidf_payer_account_type.transform([row.payer_account_type])
    payer_vpa_vec = tfidf_payer_vpa.transform([row.payer_vpa])
    payee_vpa_vec = tfidf_payee_vpa.transform([row.payee_vpa])

    # Combine all features into a single vector
    feature_vector = Vectors.dense(
        payer_name_vec.toarray()[0].tolist() +
        payee_name_vec.toarray()[0].tolist() +
        payee_account_type_vec.toarray()[0].tolist() +
        payer_account_type_vec.toarray()[0].tolist() +
        payer_vpa_vec.toarray()[0].tolist() +
        payee_vpa_vec.toarray()[0].tolist()
    )

    # Predict
    category_level1 = clf_cat1.predict([feature_vector])[0]
    category_level2 = clf_cat2.predict([feature_vector])[0]

    return (category_level1, category_level2)

# Use VectorAssembler to assemble the features into a single vector column
feature_cols = ["payer_name", "payee_name", "payee_account_type", "payer_account_type", "payer_vpa", "payee_vpa"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Create a pipeline for preprocessing and prediction
pipeline = Pipeline(stages=[vector_assembler])
model = pipeline.fit(df2)
result_df = model.transform(df2)

# Apply the preprocess_and_predict function to each row
result_df = result_df.withColumn("predictions", col("features")).rdd.map(preprocess_and_predict).toDF(["category_level1", "category_level2"])

# Join the predictions with the original DataFrame
result_df = df2.join(result_df, df2.index == result_df.index, "left")

# Show the resulting DataFrame
result_df.show()
