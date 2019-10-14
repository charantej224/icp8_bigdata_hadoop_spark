val inputfile = sc.textFile ("manager.txt")
val map = inputfile.map(line => (line.split(" ")(0),line.split(" ")(1))).reduceByKey ( (a,b) =>  a + " , " + b)
map.saveAsTextFile("reduce")
System.exit(0)