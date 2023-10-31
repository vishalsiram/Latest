import pandas as pd
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType

# Step 1: Read the CSV file using pandas
keywords_df = pd.read_csv("/path/to/your/cleaned_classification_keywords.csv")

# Step 2: Convert the pandas DataFrame to a dictionary for keyword lookup
keywords_dict = {}
for index, row in keywords_df.iterrows():
    keywords_str = str(row['KEYWORDS'])
    try:
        keywords_list = ast.literal_eval(keywords_str)
        for keyword in keywords_list:
            keywords_dict[keyword.lower()] = (row['CATEGORY_LEVEL1'], row['CATEGORY_LEVEL2'])
    except (ValueError, SyntaxError) as e:
        print("Error in row {}: {}".format(index, row))
        print("Error: {}".format(e))
        print("Skipping this row...\n")

# Step 3: Initialize a Spark session
spark = SparkSession.builder.appName("Classification").getOrCreate()

# Function to set category levels
def set_category_levels(remitter_name, base_txn_text, benef_name):
    remitter_name = '' if remitter_name is None else remitter_name.lower()
    base_txn_text = '' if base_txn_text is None else base_txn_text.lower()
    benef_name = '' if benef_name is None else benef_name.lower()

    if all(x == '' for x in [remitter_name, base_txn_text, benef_name]):
        return ('MIS TRANSACTION', 'MIS TRANSACTION')
    elif remitter_name != '' and benef_name != '' and \
         any(word in benef_name.split() for word in remitter_name.split()):
        if any(keyword in base_txn_text for keyword in ['ola', 'uber', 'rent', 'purchase', 'taxi']):
            return ('SELF TRANSFER', 'SHOPPING/RENT/CAB RENT')
        else:
            return ('SELF TRANSFER', 'PERSONAL TRANSFER')
    elif remitter_name == benef_name:
        return ('SELF TRANSFER', 'PERSONAL TRANSFER')
    elif 'gifts' in base_txn_text or 'purchase' in base_txn_text:
        return ('GIFT', 'GIFTS/PURCHASES')
    elif 'credit card' in base_txn_text or 'credit card bill' in base_txn_text or \
         'credit card' in benef_name or 'credit card bill' in benef_name:
        return ('BILLS', 'CREDIT CARD BILLS/BILL PAYMENTS')
    elif 'taxi' in base_txn_text or 'uber' in base_txn_text or 'ola' in base_txn_text:
        return ('CAB PAYMENT', 'CAB RENTAL')
    elif 'shopping' in base_txn_text or 'online shopping' in base_txn_text or 'amazon' in base_txn_text or 'flipkart' in base_txn_text:
        return ('SHOPPING', 'SHOPPING/LIFESTYLE')
    else:
        # New logic using keywords_dict
        for keyword in keywords_dict:
            if keyword in remitter_name or keyword in base_txn_text or keyword in benef_name:
                return keywords_dict[keyword]
                
    return ('OTHER TRANSFER', 'OTHER')

# Correcting the returnType to be StructType
schema = StructType([
    StructField("category_level1", StringType(), False),
    StructField("category_level2", StringType(), False)
])

# Register UDF for category levels
category_udf = udf(set_category_levels, schema)

# Define the UDF for classification
def classify_transaction(benef_ifsc, benef_account_no, source, benef_name):
    if benef_ifsc and benef_ifsc.startswith("YESB") and benef_account_no and len(str(benef_account_no)) == 15:
        return 'ybl_corporate' if source == 'current' else 'ybl_ind' if source == 'saving' else None
    elif benef_ifsc and not benef_ifsc.startswith("YESB"):
        keywords = ["pvt ltd", "innovation", "tata", "steel", "industry", "llp", "corporation", "institutaional", "tech", "automobiles", "services", "telecomunication", "travels"]
        if any(keyword in benef_name.lower() for keyword in keywords):
            return 'non_ybl_cor' if source == 'current' else 'non_ybl_ind' if source == 'saving' else None
    return None

classify_transaction_udf = udf(classify_transaction, StringType())

# Sample data for transaction DataFrame (replace this with your actual data)
data = [
    ("John Doe", "Credit Card Payment", "Bank", "YESB0000001", "123456789012345", "current", "XYZ Corp"),
    ("Jane Doe", "Shopping at Amazon", "Amazon", "HDFC0000001", "1234567890123", "saving", "ABC Pvt Ltd"),
    # ... other rows ...
]

columns = ["remitter_name", "base_txn_text", "benef_name", "benef_ifsc", "benef_account_no", "source", "benef_name"]

df = spark.createDataFrame(data, columns)

# Apply UDFs to DataFrame
df = df.withColumn('cor_ind_benf', classify_transaction_udf(df['benef_ifsc'], df['benef_account_no'], df['source'], df['benef_name']))
df = df.withColumn('categories', category_udf('remitter_name', 'base_txn_text', 'benef_name'))
df = df.withColumn('category_level1', col('categories').getItem('category_level1'))
df = df.withColumn('category_level2', col('categories').getItem('category_level2'))
df = df.drop('categories')

# Show the DataFrame
df.show()
