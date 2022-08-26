from pyspark import SparkContext, SparkConf, sqlContext
from pyspark.sql import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# appName = "PySpark SQL Server Example - via JDBC"
# master = "local"
# conf = SparkConf()\
#     .setAppName(appName) \
#     .setMaster(master) \
#     .set("spark.driver.extraClassPath","C:/Users/KY2910_krishna/PycharmProjects/pyspark/mssql-jdbc-7.2.2.jre8.jar")
# sc = SparkContext(conf=conf)
# jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# sqlContext = SQLContext(sc)
# spark = sqlContext.sparkSession

mydf = sqlContext.read.csv("C:\\Users\\KY2910_krishna\\Downloads\\test.csv",header=True)
mydf.show()
jdbcHostname = "sqlserver45.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "ky2910"
properties = {
 "user" : "ky2910",
 "password" : "Krishna@2000" }
url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname,jdbcPort,jdbcDatabase)
myfinaldf = DataFrameWriter(mydf)
myfinaldf.jdbc(url=url, table= "testing", mode ="overwrite", properties = properties)

# multiline_df.write.format("jdbc").option("url", "jdbc:sqlserver://sqlserver45.database.windows.net:1433;database=ky2910;"
# #                                                 "user=ky2910@sqlserver45;password=Krishna@2000)").option("dbtable", "testing").\
# #     option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").save()
# multiline_df.write.format("jdbc") \
#   .mode("overwrite") \
#   .option("url", jdbcUrl) \
#   .option("dbtable", "dbo.Employees2") \
#   .option("user", user) \
#   .option("password", password) \
#   .save()
# multiline_df.write.format("jdbc").options(driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
#                                    user="ky2910",
#                                    password="Krishna@2000",
#                                    url="jdbc:sqlserver://sqlserver45.database.windows.net:1433;database=ky2910",
#                                    dbtable="testing").save()