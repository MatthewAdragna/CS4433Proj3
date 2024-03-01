import org.apache.rdd.RDD
import org.apache.spark.(SparkConf, SparkContext)
import org.apache.spark.sql.SparkSession
import java.io.File
import scala.io.Source
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, GBTRegressor}

object SparkSQLQuery1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Filter Purchases")
      .master("local[*]")
      .getOrCreate()

    val purchasesDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/Users/ccordobaescobar/Downloads/Purchases.csv")

    purchasesDF.createOrReplaceTempView("purchases")

    val filteredPurchasesDF = spark.sql("SELECT * FROM purchases WHERE TransTotal <= 800")

    // Assuming you want to coalesce the dataframe into a single partition before writing
    filteredPurchasesDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("/Users/ccordobaescobar/Desktop/Trial2/T1_temp")

    // Code to rename the part file to a specific filename
    val tempDir = new File("/Users/ccordobaescobar/Desktop/Trial2/T1_temp")
    val tempCsvFile = tempDir.listFiles().find(_.getName.endsWith(".csv")).get
    tempCsvFile.renameTo(new File("/Users/ccordobaescobar/Downloads/T1.csv"))

    spark.stop()
  }
}

object SparkSQLQuery2{
    def main(args; Array[String]): Unit = (
        val spark = SparkSession.builder()
        .appName("Filter Purchases")
        .master("local[*]")
        .getOrCreate()

        val T1DF = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("/Users/ccordobaescobar/Downloads/T1.csv")

        T1DF.createOrReplaceTempView("T1")

        val groupedPurchaseDF = spark.sql("SELECT TransNumItems, " +
        "percentile_approx(TransTotal, 0.5) AS median,"+
        "MIN(TransTotal) AS min," +
        "MAX(TransTotal) AS max," +
        "FROM T1" +
        "GROUP BY TransNumItems")

        groupedPurchasesDF.show()

        spark.stop()
    )
}

object SparkSQLQuery3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Filter Purchases")
      .master("local[*]")
      .getOrCreate()

    val T1DF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/Users/ccordobaescobar/Downloads/T1.csv")

    T1DF.createOrReplaceTempView("T1")

    val customersDF = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("/Users/ccordobaescobar/Downloads/Customers.csv")

    customersDF.createOrReplaceTempView("customers")

    val groupedPurchasesDF = spark.sql("""
      SELECT ID, Age, COUNT(TransNumItems) AS TotalItems, SUM(TransTotal) AS TotalSpent
      FROM T1
      JOIN customers ON T1.CustID = customers.ID
      WHERE Age BETWEEN 18 AND 25
      GROUP BY ID, Age
    """)

    val tempDir = new File("/Users/ccordobaescobar/Desktop/Trial2/T3_temp")
    groupedPurchasesDF.repartition(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(tempDir.getPath)

    val tempCsvFile = tempDir.listFiles().find(_.getName.endsWith(".csv")).get
    tempCsvFile.renameTo(new File("/Users/ccordobaescobar/Downloads/T3.csv"))

    spark.stop()
  }
}
