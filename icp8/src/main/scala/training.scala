import org.apache.spark.{SparkConf, SparkContext}

object training {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "/home/charan/workspaces/big_data_programming")
    val conf = new SparkConf().setAppName("secondarysort").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(Array("Charan Tej,Thota","Phil,Harrison","Brian,Langdon","Bill,Chadwick","Jeff, O'Toole"))
    val pairsRDD = data.map(_.split(",")).map { k => (k(0), k(1)) }
    val numReducers = 1;
    pairsRDD.saveAsTextFile("pairsRDD")
    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))
    listRDD.saveAsTextFile("listRDD")
    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    resultRDD.saveAsTextFile("outputs/secondary_sort1")

/*    val data = sc.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
    val group = data.groupByKey().collect()
    group.foreach(println)*/

/*    val training = sc.textFile("inputs/training.txt")
    val wordsUpperCase = training.flatMap(splitAndUpper).map(f => f.toUpperCase())
    wordsUpperCase.saveAsTextFile("outputs/training")
    println(wordsUpperCase.count())*/
  }

  def splitAndUpper(input: String): Array[String] = {
    input.split(" ")
  }
}
