from pyspark.sql import SparkSession,Row
from pyspark.sql.types import StructType,StructField, StringType,ArrayType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
schema = StructType([
      StructField("firstName",StringType(),True),
      StructField("lastName",StringType(),True),
      StructField("gender",StringType(),True),
      StructField("age",StringType(),True),
      StructField("address",StructType([
      StructField("streetAddress",StringType(),True),
      StructField("city",StringType(),True),
      StructField("state",StringType(),True),
      ])),
      StructField("phoneNumbers",ArrayType(StructType([StructField("type",StringType(),True),StructField("number",StringType(),True)])),
                  True)])

schema = StructType([StructField("name",ArrayType(StringType()),True),StructField("ages",ArrayType(StringType()),True)])
df_with_schema = spark.read.schema(schema).json("C:\\Users\\KY2910_krishna\\PycharmProjects\\pyspark\\venv\\parse1.json")
df_with_schema.printSchema()
df_with_schema.show()
#sql
df.createOrReplaceTempView("tableA")
spark.sql("select * from test where salary<4000").show()
spark.sql("SELECT count(*) from tableA").show()
#parsing parse1
df_with_schema.withColumn("test", F.explode(F.arrays_zip("name", "ages"))).select("test.Price", "test.Product").show()
df.withColumn("test",F.arrays_zip("users.name","users.age")).withColumn("test",F.explode("test")).select(F.col("test.name").alias("names"),F.col("test.age").alias("ages")).show()

#sql on dataframes
df.groupBy("gender").count().show()
df.select("*").orderBy("salary").show()





