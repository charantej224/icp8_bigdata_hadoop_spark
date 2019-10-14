val inputfile = sc.textFile ("charan.txt")
val flatmap = inputfile.flatMap (line => line.split(" "))
flatmap.saveAsTextFile("flatmap")
val maps = flatmap.map(word => (word, 1))
maps.saveAsTextFile("maps")
val counts = maps.reduceByKey (_+_)
counts.saveAsTextFile("output")
System.exit(0)

