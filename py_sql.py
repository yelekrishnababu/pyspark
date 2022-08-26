from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Create spark configuration object
conf = SparkConf()
conf.setMaster("local").setAppName("My app")

# Create spark context and sparksession
sc = SparkContext.getOrCreate(conf=conf)
spark = SparkSession(sc)

# read csv file in a dataframe
df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("C:/Users/KY2910_krishna/PycharmProjects/pyspark/test.csv")
df.show()
# set variable to be used to connect the database
database = "ky2910"
table = "dbo.tbl_spark_df"
user = "ky2910"
password = "Krishna@2000"

# write the dataframe into a sql table
df.write.mode("overwrite") \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://sqlserver45.database.windows.net:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()