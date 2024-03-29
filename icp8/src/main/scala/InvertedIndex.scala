import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "/home/charan/workspaces/big_data_programming")
    val conf = new SparkConf().setAppName("Spark - inverted Index").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Read file containing stopwords into a broadcast variable
    val stopWordsInput = sc.textFile("inputs/stopwords.txt")
    val stopWords = stopWordsInput.flatMap(x => x.split("\\r?\\n")).map(_.trim)
    val broadcastStopWords = sc.broadcast(stopWords.collect.toSet)


    // Read input file and filter all stopwords
    sc.wholeTextFiles("inputs/shakespeare/").flatMap {
      case (path, text) =>
        text.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")
          .split("""\W+""")
          .filter(!broadcastStopWords.value.contains(_)) map {
          // Create a tuple of (word, filePath)
          word => (word, path)
        }
    }.map {
      // Create a tuple with count 1 ((word, fileName), 1)
      case (w, p) => ((w, p.split("/")(6)), 1)
    }.reduceByKey {
      // Group all (word, fileName) pairs and sum the counts
      case (n1, n2) => n1 + n2
    }.map {
      // Transform tuple into (word, (fileName, count))
      case ((w, p), n) => (w, (p, n))
    }.groupBy {
      // Group by words
      case (w, (p, n)) => w
    }.map {
      // Output sequence of (fileName, count) into a comma seperated string
      case (w, seq) =>
        val seq2 = seq map {
          case (_, (p, n)) => (p, n)
        }
        (w, seq2.mkString(", "))
    }.saveAsTextFile("outputs/inverted_index")
  }
}
