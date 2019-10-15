import org.apache.spark.{SparkConf, SparkContext}

object Manager {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/home/charan/workspaces/big_data_programming")
    val conf = new SparkConf().setAppName("secondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputfile = sc.textFile("inputs/manager.txt")
    val map = inputfile.map(line => (line.split(" ")(0), line.split(" ")(1))).reduceByKey((a, b) => a + " , " + b)
    map.saveAsTextFile("outputs/manager")
  }

}
