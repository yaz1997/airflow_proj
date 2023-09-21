from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
from dotenv import load_dotenv


# encrypting username and password
load_dotenv()

user = os.getenv("postgres_username")
password = os.getenv("postgres_password")

spark = SparkSession.builder.appName("airflow").getOrCreate()

table_names = ["api_to_pg"]
table_dataframes = {}

# jdbc_url = "jdbc:postgresql://localhost:5432/airflow"
# connection_properties = {
#     "user": 'postgres',
#     "password": 'fuseriyaz97',
#     "driver": "org.postgresql.Driver"
# }

api_to_pg = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/airflow") \
    .option("dbtable", "api_to_pg") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .load()
api_to_pg.show(truncate=False)


filtered_df = api_to_pg.filter(F.col("duration") == "hours")

result = filtered_df.groupBy("accessibility").agg(F.count("*").alias("count"))
result.show()


result.write.parquet("/home/riyaz/airflow/proj_data/api_data_par.parquet")

result.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/airflow',driver = 'org.postgresql.Driver', dbtable = 'Question_1', user=user,password=password).mode('overwrite').save()
