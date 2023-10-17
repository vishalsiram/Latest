# Check for None values and convert to lowercase
for col_name in ["payer_name", "payee_name", "payee_account_type", "payer_account_type", "payer_vpa", "payee_vpa"]:
    df2 = df2.withColumn(col_name, col(col_name).cast("string").lower())
