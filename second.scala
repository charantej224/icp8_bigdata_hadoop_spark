import org.apache.spark.{SparkConf, SparkContext}

object second {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "/home/charan/")
    val conf = new SparkConf().setAppName("secondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("input2.txt")
    val pairsRDD = personRDD.map(_.split(",")).map { k => ((k(0), k(1)),k(2)) }
    println("pairsRDD")
    pairsRDD.foreach {
      println
    }
    val numReducers = 3;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(d => d))
    println("listRDD")
    listRDD.foreach {
      println
    }
    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    println("resultRDD")
    resultRDD.foreach {
      println
    }

    resultRDD.saveAsTextFile("output2")

  }
}
