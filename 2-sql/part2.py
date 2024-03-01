from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


def main():
    spark = SparkSession.builder \
        .appName("Part2-1") \
        .getOrCreate()

    customers_schema = StructType([
        StructField("ID", IntegerType(), nullable=False),
        StructField("Name", StringType(), nullable=False),
        StructField("Age", IntegerType(), nullable=False),
        StructField("CountryCode", IntegerType(), nullable=False),
        StructField("Salary", FloatType(), nullable=False)
    ])
    purchases_schema = StructType([
        StructField("TransID", IntegerType(), nullable=False),
        StructField("CustID", IntegerType(), nullable=False),
        StructField("TransTotal", FloatType(), nullable=False),
        StructField("TransNumItems", IntegerType(), nullable=False),
        StructField("TransDesc", StringType(), nullable=False)
    ])

    customers = spark.read.schema(customers_schema).csv("hdfs:///user/cs4433/project3/input/customers.csv")
    customers.createOrReplaceTempView("customers")
    purchases = spark.read.schema(purchases_schema).csv("hdfs:///user/cs4433/project3/input/purchases.csv")
    purchases.createOrReplaceTempView("purchases")

    t1 = spark.sql("SELECT * FROM purchases WHERE TransTotal <= 800")
    t1.write.csv("hdfs:///user/cs4433/project3/output/part2a/1.csv", header=True, mode="overwrite")

    t1.createOrReplaceTempView("T1")
    t1.show(50)

    part2 = spark.sql("SELECT TransNumItems, " +
                      "percentile_approx(TransTotal, 0.5) AS median," +
                      "MIN(TransTotal) AS min," +
                      "MAX(TransTotal) AS max," +
                      "FROM T1" +
                      "GROUP BY TransNumItems")
    part2.show(50)

    part2.write.csv("hdfs:///user/cs4433/project3/output/part2a/2.csv", header=True, mode="overwrite")

    part3= spark.sql(
        " SELECT ID, Age, COUNT(TransNumItems) AS TotalItems, SUM(TransTotal) AS TotalSpent" +
        "FROM T1" +
        "JOIN customers ON T1.CustID = customers.ID" +
        "WHERE Age BETWEEN 18 AND 25" +
        "GROUP BY ID, Age")

    part3.show(50)

    part3.write.csv("hdfs:///user/cs4433/project3/output/part2a/3.csv", header=True, mode="overwrite")

if __name__ == "__main__":
    main()
