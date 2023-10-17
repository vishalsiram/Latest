
TypeErrorTraceback (most recent call last)
<ipython-input-24-2f5b58cffd22> in <module>()
     25 # Check for None values and convert to lowercase
     26 for col_name in ["payer_name", "payee_name", "payee_account_type", "payer_account_type", "payer_vpa", "payee_vpa"]:
---> 27     df2 = df2.withColumn(col_name, col(col_name).cast("string").lower())
     28 
     29 # Define a function to preprocess and predict the categories

TypeError: 'Column' object is not callable
