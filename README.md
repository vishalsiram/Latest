# coding: utf-8

# In[1]:

import pandas as pd
from nltk.util import skipgrams

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import udf
from pyspark.sql import *
from pyspark.ml import feature as MF
from dateutil import relativedelta
import datetime
# In[2]:



# In[62]:


# In[ ]:

#rtgs_data_acct_ind.count()


# In[ ]:


sc = SparkContext()
sc.setCheckpointDir('/tmp/spark-code-rtgs')
try:
    # Try to access HiveConf, it will raise exception if Hive is not added
    sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
except py4j.protocol.Py4JError:
    sqlContext = SQLContext(sc)
except TypeError:
    sqlContext = SQLContext(sc)



# In[2]:


t1 ='db_smith.smth_pool_base_rtgs'
t2='db_stage.stg_fle_category_master'


# In[57]:

import ConfigParser
import sys
configFile = sys.argv[1]
#configFile = '/data/08/notebooks/tmp/Anisha/TransactionClassification/smth_pool_rtgs_20190222200554_python.ini'
config = ConfigParser.ConfigParser()
config.read(configFile)


# In[58]:

data_dtt = config.get('default', 'MASTER_DATA_DATE_KEY').replace("'",'').replace('"','')
t1 = config.get('default','INP_DB_NM_1') + '.' +      config.get('default','INP_TBL_NM_1')

t2 = config.get('default', 'INP_DB_NM_2') + '.' +      config.get('default','INP_TBL_NM_2')
t2_batch = config.get('default', 'END_BATCH_ID_2')

output_tbl=config.get('default', 'OUT_DB_NM') + '.' +             config.get('default','OUT_TBL_NM')

#data_dtt='2019-08-19'


# In[14]:

rtgs_data=(sqlContext.table(t1).filter((F.col('data_dt')>=( datetime.datetime.strptime(data_dtt, "%Y-%m-%d")-relativedelta.relativedelta(days=7)))
											#&(F.col('data_dt')<=str(data_dtt))
											).drop('benef_id','self_flag'))

category_master=sqlContext.table(t2)


# In[15]:

def replaceNull(df, col_list,default_value=''):
    for col in col_list:
        df = df.withColumn(col,F.when(F.col(col).isNull(),default_value).otherwise(F.col(col)))
    return df

col_list1 = ['base_txn_text','benef_nickname','rmtr_to_bene_note']
rtgs_data1 = replaceNull(rtgs_data,col_list1)


# In[16]:

rtgs_data2=rtgs_data1.withColumn('Remarks',(F.upper(F.concat((F.regexp_replace(F.col('base_txn_text'),'(\d+)','')),
                                                           #F.lit(' '),
                                                           #(F.regexp_replace(F.col('derived_txn_txt'),'(\d+)','')),
                                                           F.lit(' '),
                                                           (F.regexp_replace(F.col('rmtr_to_bene_note'),'(\d+)',''))))))


# In[17]:

rtgs_data3=rtgs_data2.withColumn('Remarks1',F.concat(F.col('Remarks'),F.lit(" "),F.col('benef_nickname')))


# In[18]:

root_path = '/ybl/dwh/artifacts/sherlock/pythonJobs'
#root_path='.'
sc.addPyFile(root_path + '/Transaction-Classification/EntityFW.py')
from EntityFW import *
kpp, regex_list = initilize_keywords(root_path,sc,['NACH', 'crowdsource', 'DD', 'Cheques', 'COMMON','TRANSFERS','Ecollect'])
sc.addPyFile(root_path +'/Transaction-Classification/RemarksFW.py')
from RemarksFW import *
R_kp, R_result_dict = R_initialize(root_path,sc)
sc.addPyFile(root_path +'/Transaction-Classification/RemarkEntityWrapper.py')
from RemarkEntityWrapper import *

df_res = ApplyFWSequence(root_path,sc,rtgs_data3,'benef_name','Remarks1', 'category_code', 'benef_id',R_kp, R_result_dict,kpp,
                         regex_list,'rb','510000', remit_col='remitter_name', self_tnfr_col='self_flag')


# In[19]:

purpose_code=sc.parallelize([('PC01','410000'),
('PC02','410000'),
('PC03','110000'),
('PC04','110000'),
('PC05','350200'),
('PC06','190000'),
('PC07','390000'),
('PC08','290400'),
('PC09','290000'),
('PC10','230000'),
('PC11','150000'),
('PC12','160000'),
('PC13','120001'),
('PC31','410000')]).toDF(['code','category_code1'])


# In[20]:

df_res1=df_res.join(F.broadcast(purpose_code),'code','left')


# In[21]:

df_res2=df_res1.withColumn('category_code',F.when((F.col('category_code').isin('510000'))
                                                  &(~F.col('category_code1').isNull()),F.col('category_code1'))
                                                                                       .otherwise(F.col('category_code')))


# In[22]:

df_res3=df_res2.withColumn('benef_name',F.when((F.col('benef_id').isNull()|F.col('benef_id').isin('')),F.col('benef_name')).otherwise(F.col('benef_id')))


# In[23]:

rtgs_final=df_res3.join(category_master,'category_code','left')


# In[7]:

rtgs_all1=rtgs_final.select(F.col("txn_ref_no"),
F.col("txn_date"),
F.col("txn_amt"),
F.col("mode"),
F.col("remitter_id"),
F.col("remitter_name"),
F.col("remitter_type"),
F.col("remitter_class"),
F.col("remitter_sub_class"),
F.col("remitter_ifsc"),
F.col("remitter_bank_name"),
F.col("remitter_account_no"),
F.col("remitter_cust_id"),
F.col("benef_id"),
F.col("benef_name"),
F.col("benef_type"),
F.col("benef_class"),
F.col("benef_sub_class"),
F.col("benef_ifsc"),
F.col("benef_bank_name"),
F.col("benef_account_no"),
F.col("benef_cust_id"),
F.col("base_txn_text"),
F.col("rmtr_to_bene_note"),
F.col("online_offline"),
F.col("category_level1"),
F.col("category_level2"),
F.col("category_level3"),
F.col("category_code"),
F.col("recurrance_flag"),
F.col("recurrance_pattern_id"),
F.col("verification_flag"),
F.col("self_flag"),
F.col("txn_ref_key"),
F.col("channel_code"),
F.col("codstatus"),
F.col("acctstatus"),
F.col("msgstatus"),
F.col("codcurr"),
F.col("datvalue"),
F.col("direction"),
F.col("msgtype"),
F.col("submsgtype"),
F.col("benef_nickname"),
F.col("utr"),
F.col("iduserreference"),
F.col("idrelatedref"),
F.col("channel"),
F.col("I_C"),
F.col('txntype'),
F.col('data_dt'))




# In[109]:

output_cols = sqlContext.table(output_tbl).columns



to_fill = rtgs_all1.columns




res = rtgs_all1
for x in output_cols:
    if x  not in to_fill:
        res = res.withColumn(x, F.lit(''))



res_to_write = res.select(output_cols)

res_to_write.write.insertInto(output_tbl, True)
