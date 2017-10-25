package org.apache.spark.examples.bigdebug

import org.apache.spark.SparkConf
import org.apache.spark.bdd.{ BDConfiguration}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
/**
  * Created by ali on 6/23/17.
  */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf();
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if(args.size < 2) {
      logFile = "/Users/satya/Desktop/text.txt"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("WordCount-" + lineage + "-" + logFile)
    val bconf = new BDConfiguration()
    bconf.ENABLE_CRASH_LATENCY = false;
    bconf.STRAGGLER_PERIODIC_REQUEST = false;
    bconf.setTerminationOnFinished(false);
    bconf.setFilePath("");
    val lc = new LineageContext(conf, bconf)
    lc.setCaptureLineage(true)
    // Job
    val file = lc.textFile(logFile, 5)
    val pairs = file.flatMap(line => line.trim().split(" ")).map{
      word =>
        if(word.contains(",")){
          //Thread.sleep(10000);
        }
        (word.trim(), 1)
    }
    val counts = pairs.reduceByKey(_ + _)
    counts.collectWithId.foreach(println)


    lc.setCaptureLineage(false)
    var linRdd = counts.getLineage()
    println(linRdd.collect().length)
    linRdd.collect().foreach(println)
    linRdd = linRdd.filter(_ == 2393178122L)
    linRdd.collect().foreach(println)
    val show = linRdd.show()
    // linRdd.collect().foreach(println)
    linRdd = linRdd.goBack()
    linRdd.collect().foreach(println)
    linRdd.show()
    linRdd = linRdd.goBack()
    linRdd.collect().foreach(println)
    linRdd.show()

    lc.sparkContext.stop()
  }
}


