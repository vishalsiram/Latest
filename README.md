import re
import pickle
from pyspark.sql import SparkSession
from pyspark.ml.feature import IDFModel
from pyspark.mllib.linalg import Vectors
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName('Optimized Code') \
    .getOrCreate()

# Load models and vectorizers and broadcast them
broadcast_vars = {
    name: spark.sparkContext.broadcast(pickle.load(open(name + ".pkl", "rb")))
    for name in [
        "clf_cat1", "clf_cat2",
        "tfidf_payer_name", "tfidf_payee_name",
        "tfidf_payee_account_type", "tfidf_payer_account_type",
        "tfidf_payer_vpa", "tfidf_payee_vpa"
    ]
}

def custom_tokenizer(text):
    pattern = re.compile(r'[a-zA-Z]+\d+')
    return pattern.findall(text)

def predict_categories(payer_name, payee_name, payee_account_type,
                       payer_account_type, payer_vpa, payee_vpa):
    global broadcast_vars

    # Transform input data using the TFIDF vectorizers
    features = [
        payer_name, payee_name,
        payee_account_type, payer_account_type,
        payer_vpa, payee_vpa
    ]

    feature_names = [
        "tfidf_payer_name", "tfidf_payee_name",
        "tfidf_payee_account_type", "tfidf_payer_account_type",
        "tfidf_payer_vpa", "tfidf_payee_vpa"
    ]

    tfidf_matrix = [
        Vectors.dense(broadcast_vars[name].value.transform([value]).toarray()[0])
        for name, value in zip(feature_names, features)
    ]

    # Concatenate TFIDF vectors
    concatenated_vector = Vectors.dense([item for sublist in tfidf_matrix for item in sublist])

    # Predict
    prediction_cat1 = broadcast_vars["clf_cat1"].value.predict([concatenated_vector])
    prediction_cat2 = broadcast_vars["clf_cat2"].value.predict([concatenated_vector])

    return prediction_cat1[0], prediction_cat2[0]

# Assuming df is your DataFrame with necessary columns
rdd = df.rdd.map(lambda row: (
    row,
    predict_categories(
        row.payer_name, row.payee_name,
        row.payee_account_type, row.payer_account_type,
        row.payer_vpa, row.payee_vpa
    )
))

# Convert the RDD back to DataFrame
result_df = spark.createDataFrame(
    rdd.map(lambda x: list(x[0]) + list(x[1])),
    df.columns + ['category_level1', 'category_level2']
)

result_df.show()

