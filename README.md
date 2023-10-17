# Define the custom tokenizer
def custom_tokenizer(text):
    pattern = re.compile(r'[a-zA-Z]+\d+')
    return pattern.findall(text)

# Load the saved models and vectorizers
clf_cat1 = pickle.load(open("clf_cat1.pkl", "rb"))
clf_cat2 = pickle.load(open("clf_cat2.pkl", "rb"))
tfidf_payer_name = pickle.load(open("tfidf_payer_name.pkl", "rb"))
tfidf_payee_name = pickle.load(open("tfidf_payee_name.pkl", "rb"))
tfidf_payee_account_type = pickle.load(open("tfidf_payee_account_type.pkl", "rb"))
tfidf_payer_account_type = pickle.load(open("tfidf_payer_account_type.pkl", "rb"))
tfidf_payer_vpa = pickle.load(open("tfidf_payer_vpa.pkl", "rb"))
tfidf_payee_vpa = pickle.load(open("tfidf_payee_vpa.pkl", "rb"))



# Define the prediction function
def predict_categories(payer_name, payee_name, payee_account_type,
                       payer_account_type, payer_vpa, payee_vpa):
    # Transform input data using the TFIDF vectorizers
    payer_name_vec = tfidf_payer_name.transform([payer_name])
    payee_name_vec = tfidf_payee_name.transform([payee_name])
    payee_account_type_vec = tfidf_payee_account_type.transform([payee_account_type])
    payer_account_type_vec = tfidf_payer_account_type.transform([payer_account_type])
    payer_vpa_vec = tfidf_payer_vpa.transform([payer_vpa])
    payee_vpa_vec = tfidf_payee_vpa.transform([payee_vpa])

    tfidf_matrix = pd.concat([pd.DataFrame(payer_name_vec.toarray()),
                              pd.DataFrame(payee_name_vec.toarray()),
                              pd.DataFrame(payee_account_type_vec.toarray()),
                              pd.DataFrame(payer_account_type_vec.toarray()),
                              pd.DataFrame(payer_vpa_vec.toarray()),
                              pd.DataFrame(payee_vpa_vec.toarray())], axis=1)

    # Predict
    prediction_cat1 = clf_cat1.predict(tfidf_matrix)
    prediction_cat2 = clf_cat2.predict(tfidf_matrix)

    return [prediction_cat1[0], prediction_cat2[0]]

# Register the prediction function as a UDF
predict_udf = udf(predict_categories, ArrayType(StringType()))

# Make sure your DataFrame has columns: payer_name, payee_name, payee_account_type, payer_account_type, payer_vpa, payee_vpa
result_df = df2.withColumn("predictions", predict_udf("payer_name", "payee_name", "payee_account_type", "payer_account_type", "payer_vpa", "payee_vpa"))
# Assuming result_df is the DataFrame with the combined predictions column
result_df = result_df.withColumn("category_level1", result_df["predictions"].getItem(0))
result_df = result_df.withColumn("category_level2", result_df["predictions"].getItem(1))

# Dropping the combined predictions column
result_df = result_df.drop("predictions")

# Show the resulting DataFrame
result_df.show()
