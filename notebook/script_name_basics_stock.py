from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from sqlalchemy import create_engine

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("your-app-name") \
    .config("spark.jars", "notebook/postgresql.jar") \
    .getOrCreate()

data_name_sp = spark.read.option("header", "true").csv("../Data/name.basics.tsv", sep="\t", inferSchema=True)
data_name_sp.show(5)

data_name_sp = data_name_sp.select(
    [F.when(F.col(c) == '\\N', None).otherwise(F.col(c)).alias(c) for c in data_name_sp.columns]
)

# PostgreSQL connection properties
# url = "jdbc:postgresql://localhost:5432/imdb_row"
# properties = {
#     "user": "postgres",
#     "password": "1234",
#     "driver": "org.postgresql.Driver"
# }

# Load the PostgreSQL table into a PySpark DataFrame
data_name_sp.write.mode("overwrite").parquet("../Data/name_basics")
df = pd.read_parquet("../Data/name_basics", engine="pyarrow")


db_url = 'postgresql+psycopg2://postgres:1234@localhost:5432/imdb_row'
engine = create_engine(db_url)
df.to_sql('name_basics', engine, if_exists='replace', index=False, chunksize=1000)

spark.stop()