import re
import pickle
import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType

# Load the saved models and vectorizers outside of the function to avoid loading them multiple times
models_and_vectorizers = {
    "clf_cat1": pickle.load(open("clf_cat1.pkl", "rb")),
    "clf_cat2": pickle.load(open("clf_cat2.pkl", "rb")),
    "tfidf_payer_name": pickle.load(open("tfidf_payer_name.pkl", "rb")),
    "tfidf_payee_name": pickle.load(open("tfidf_payee_name.pkl", "rb")),
    "tfidf_payee_account_type": pickle.load(open("tfidf_payee_account_type.pkl", "rb")),
    "tfidf_payer_account_type": pickle.load(open("tfidf_payer_account_type.pkl", "rb")),
    "tfidf_payer_vpa": pickle.load(open("tfidf_payer_vpa.pkl", "rb")),
    "tfidf_payee_vpa": pickle.load(open("tfidf_payee_vpa.pkl", "rb"))
}

def custom_tokenizer(text):
    pattern = re.compile(r'[a-zA-Z]+\d+')
    return pattern.findall(text)

def predict_categories(payer_name, payee_name, payee_account_type,
                       payer_account_type, payer_vpa, payee_vpa):
    # Transform input data using the TFIDF vectorizers
    tfidf_matrices = []
    for feature, value in zip(["payer_name", "payee_name", "payee_account_type",
                               "payer_account_type", "payer_vpa", "payee_vpa"],
                              [payer_name, payee_name, payee_account_type,
                               payer_account_type, payer_vpa, payee_vpa]):
        vectorizer = models_and_vectorizers["tfidf_" + feature]
        tfidf_matrices.append(pd.DataFrame(vectorizer.transform([value]).toarray()))

    tfidf_matrix = pd.concat(tfidf_matrices, axis=1)

    # Predict
    prediction_cat1 = models_and_vectorizers["clf_cat1"].predict(tfidf_matrix)
    prediction_cat2 = models_and_vectorizers["clf_cat2"].predict(tfidf_matrix)

    return [prediction_cat1[0], prediction_cat2[0]]

# Register the prediction function as a UDF
predict_udf = udf(predict_categories, ArrayType(StringType()))

# Assuming df2 is your DataFrame with the necessary columns
result_df = df2.withColumn("predictions", predict_udf("payer_name", "payee_name", "payee_account_type", "payer_account_type", "payer_vpa", "payee_vpa"))

# Split predictions into separate columns
result_df = result_df.withColumn("category_level1", result_df["predictions"].getItem(0))
result_df = result_df.withColumn("category_level2", result_df["predictions"].getItem(1))

# Dropping the combined predictions column
result_df = result_df.drop("predictions")

# Show the resulting DataFrame
result_df.show()
