package DataFrameCSV

import org.apache.spark.sql.SparkSession

object DataFrameCSV {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").appName("data frame for csv").getOrCreate()
    val dataframe = sparkSession.read.csv("inputs/family.csv")
    dataframe.printSchema()
    dataframe.show()

    val dataframe1 = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv("inputs/family.csv")
    dataframe1.printSchema()
    dataframe1.show()

    /*val dataframe2 = sparkSession.read.json("inputs/family.json")
    dataframe2.printSchema()
    dataframe2.show()*/

  }
}
