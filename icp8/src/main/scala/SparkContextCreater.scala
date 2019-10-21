import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object SparkContextCreater {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("sample").master("local[*]").getOrCreate();
    val intArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 8, 9)
    val intRdd = sparkSession.sparkContext.parallelize(intArray, 2)
    println(intRdd.count())
    intRdd.distinct().collect().foreach(println)

    val schema = StructType(
      StructField("Serial numbers", IntegerType, true) :: Nil
    )
    val rowRdd = intRdd.map(Row(_))

    val df = sparkSession.createDataFrame(rowRdd, schema)
    df.printSchema()
    df.show()

  }
}
