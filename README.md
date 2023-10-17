import re
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline

# Initialize a Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Load the saved models and vectorizers (broadcast them for efficiency)
clf_cat1 = spark.sparkContext.broadcast(pickle.load(open("clf_cat1.pkl", "rb")))
clf_cat2 = spark.sparkContext.broadcast(pickle.load(open("clf_cat2.pkl", "rb")))
tfidf_payer_name = spark.sparkContext.broadcast(pickle.load(open("tfidf_payer_name.pkl", "rb")))
tfidf_payee_name = spark.sparkContext.broadcast(pickle.load(open("tfidf_payee_name.pkl", "rb")))
tfidf_payee_account_type = spark.sparkContext.broadcast(pickle.load(open("tfidf_payee_account_type.pkl", "rb")))
tfidf_payer_account_type = spark.sparkContext.broadcast(pickle.load(open("tfidf_payer_account_type.pkl", "rb")))
tfidf_payer_vpa = spark.sparkContext.broadcast(pickle.load(open("tfidf_payer_vpa.pkl", "rb")))
tfidf_payee_vpa = spark.sparkContext.broadcast(pickle.load(open("tfidf_payee_vpa.pkl", "rb")))

# Assuming df2 is your input DataFrame
# Define a function to preprocess and predict the categories
def preprocess_and_predict(payer_name, payee_name, payee_account_type,
                            payer_account_type, payer_vpa, payee_vpa):
    # Check for None values and convert to lowercase
    payer_name = payer_name.lower() if payer_name else ""
    payee_name = payee_name.lower() if payee_name else ""
    payee_account_type = payee_account_type.lower() if payee_account_type else ""
    payer_account_type = payer_account_type.lower() if payer_account_type else ""
    payer_vpa = payer_vpa.lower() if payer_vpa else ""
    payee_vpa = payee_vpa.lower() if payee_vpa else ""

    # Transform input data using the TFIDF vectorizers
    payer_name_vec = tfidf_payer_name.value.transform([payer_name])
    payee_name_vec = tfidf_payee_name.value.transform([payee_name])
    payee_account_type_vec = tfidf_payee_account_type.value.transform([payee_account_type])
    payer_account_type_vec = tfidf_payer_account_type.value.transform([payer_account_type])
    payer_vpa_vec = tfidf_payer_vpa.value.transform([payer_vpa])
    payee_vpa_vec = tfidf_payee_vpa.value.transform([payee_vpa])

    # Combine all features into a single vector
    feature_vector = Vectors.dense(
        payer_name_vec.toarray()[0].tolist() +
        payee_name_vec.toarray()[0].tolist() +
        payee_account_type_vec.toarray()[0].tolist() +
        payer_account_type_vec.toarray()[0].tolist() +
        payer_vpa_vec.toarray()[0].tolist() +
        payee_vpa_vec.toarray()[0].tolist()
    )

    return [str(clf_cat1.value.predict([feature_vector])[0]), str(clf_cat2.value.predict([feature_vector])[0])]

# Define the prediction UDF
predict_udf = udf(preprocess_and_predict, ArrayType(StringType()))

# Make sure your DataFrame has columns: payer_name, payee_name, payee_account_type, payer_account_type, payer_vpa, payee_vpa
result_df = df2.withColumn("predictions", predict_udf("payer_name", "payee_name", "payee_account_type", "payer_account_type", "payer_vpa", "payee_vpa"))

# Add the "category_level1" and "category_level2" columns
result_df = result_df.withColumn("category_level1", result_df["predictions"].getItem(0))
result_df = result_df.withColumn("category_level2", result_df["predictions"].getItem(1))

# Dropping the combined predictions column
result_df = result_df.drop("predictions")

# Show the resulting DataFrame
result_df.show()

