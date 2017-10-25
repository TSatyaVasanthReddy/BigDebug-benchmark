/**
  * Author : satyavasanth@ucla.edu
  * Spark Map Reduce program to find the Single Source Shortest path given a graph and source vertex
  */

package org.apache.spark.examples.bigdebug

import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._;
object SSSP{
  val conf = new SparkConf().setAppName("SSSP M/R").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val includeDelay = false
  def main(args: Array[String]){
    var logger = Logger.getLogger(getClass().getName());
    logger.setLevel(org.apache.log4j.Level.DEBUG)

    /*Input file with adjacency list 
      Each line is :
      a b,c,d
      It means there are edges from a to b,c and d
    */

    val filePath = "/Users/satya/Desktop/graph.txt"
    var reduced = null
    val graphText = sc.textFile(filePath)
    val pw = new PrintWriter(new File("SSSP.log" ))
    pw.write("hi")
    //TODO take from arguments
    val source = "1"
    var n = 7
    //
    //The value will be the adjacent edges separated by comma as a string
    var graphWithoutSplit: org.apache.spark.rdd.RDD[((String, Int), String)] = graphText.map(line => if (line.split(" ")(0).equals(source))
      ((line.split(" ")(0), 0), line.split(" ")(1))
    else
      ((line.split(" ")(0), Integer.MAX_VALUE), line.split(" ")(1))
    )
    var i = 1
    //For multiple iterations
    while(i<n) {
      //Splitting the adjacency list in String into an Array list of vertices
      var graphWithSplit = graphWithoutSplit.map(line => (line._1, line._2.split(",")))
      //If we have <(i,d),(j1,j2,j3)>, emit ((j1,d+1),""),((j2,d+2),"")...
      var preMapOutput = graphWithSplit.flatMapValues(values => values).map(
        pair => if (pair._1._2 == Integer.MAX_VALUE){
          if(includeDelay && pair._1._1.toInt%3 ==0){
            Thread.sleep(500)
          }
          ((pair._2, Integer.MAX_VALUE), "")
        }
        else
          {
            if(includeDelay && pair._1._1.toInt%3 ==0){
              Thread.sleep(500)
            }
            ((pair._2, pair._1._2 + 1), "")
          }).filter(pair => pair._1._1.length > 0)

      //Union it with the initial RDD
      var mapOutput1 = preMapOutput.union(graphWithoutSplit)
     //mapOutput1.foreach(println)
      //Transform into required key,value form
      var mapOutput = mapOutput1.map(pair => (pair._1._1, (pair._1._2, pair._2)))
     // mapOutput.foreach(println)
      var grpBykey = mapOutput.groupByKey()
      pw.write("Iteration "+i+" grpBykey\n")
      grpBykey.collect().foreach(x => {
        pw.write(x._1+" "+x._2.toString()+"\n")
      })
      //Reduce Step
      var abc = 44
      var reduced = mapOutput.reduceByKey((val1, val2) => if (val1._2.length > val2._2.length) (Integer.min(val1._1, val2._1), val1._2) else (Integer.min(val1._1, val2._1), val2._2))
      graphWithoutSplit = reduced.map(line => ((line._1, line._2._1), line._2._2))
     // println("After Iteration "+i+" the distances of the vertices from "+source)
      var output = reduced.map(pair=>(pair._1,pair._2._1)).collect()
      pw.write("Reduced Output "+i+"\n")
      reduced.collect().foreach(x => {
        pw.write(x._1+" "+x._2.toString()+"\n")
      })
      var end = true
      output.foreach(x=> {
        if(x._2 == i ){
          end = false
        }
        //println(x.toString())
      })
      i+=1
      if(end){
        i = Integer.MAX_VALUE
      }

    }
    pw.close();

  }

}