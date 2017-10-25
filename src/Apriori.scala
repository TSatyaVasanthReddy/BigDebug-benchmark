/**
  * Author: satyavasanth@ucla.edu
  * Original : https://github.com/arzmuz/apriori-frequent-itemsets-association-rules/blob/master/src/main/scala/se/kth/spark/hw2/main/Main.scala
  */

package org.apache.spark.examples.bigdebug

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main {

  val conf = new SparkConf().setAppName("Apriori M/R").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val addDelay = false

  // Main function - may change variables here
  def main(args: Array[String])
  {
    val support = 1000
    val filePath = "/Users/satya/Desktop/Fall17/GSR-F17/T10I4D100K.dat.txt"
    val transactions = sc.textFile(filePath)
    val numOfTransactions: Double = transactions.count
    // First run pass k=1 to get frequent items
    val L_1: org.apache.spark.rdd.RDD[(String, Int)] = getOneItemCounts(transactions, support)
    val freqItems = L_1.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
    var L_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)] = L_1
    var k: Int = 2
    // Then run passes k=2,.. until no more frequent itemsets can be generated.
    while(!L_kMinusOne.isEmpty())
    {
      if(k%3==0){
        Thread.sleep(1000)
      }
      val freqCombos: org.apache.spark.rdd.RDD[String] = generateCandidatesSet(L_kMinusOne, freqItems)
      val L_k: org.apache.spark.rdd.RDD[(String, Int)] = generateL_k(transactions, freqCombos, support, k)
      L_kMinusOne = L_k
      k += 1
    }
  }


  // Fetch support value for a given tuple
  def getSupportFromL_k(L_kSet: Set[(String, Int)], keyToFind: String): String =
  {
    val support = L_kSet.filter{ case (s,i) => s == keyToFind }.map{ case (s,i) => i }
    val supportStr = support.mkString("")
    return supportStr
  }

  def getOneItemCounts(transactions: org.apache.spark.rdd.RDD[String], support: Int): org.apache.spark.rdd.RDD[(String, Int)] =
  {
    // Pass 1
    val items = transactions.flatMap(txn => txn.split(" "))
    val itemOnes = items.map(item => ("["+item+"]", 1))
    val itemCounts_C1 = itemOnes.reduceByKey(_+_)
    val itemCounts_L1 = itemCounts_C1.filter{case (item,sup) => sup >= support}
    println("\n1 Frequent items:")
    itemCounts_L1.foreach(println)
    return itemCounts_L1
  }

  def generateCandidatesSet(L_kMinusOne: org.apache.spark.rdd.RDD[(String, Int)], freqItems_L1: org.apache.spark.rdd.RDD[String])
  :org.apache.spark.rdd.RDD[String] =
  {
    val tupleLength = L_kMinusOne.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }.top(1)
    val desiredTupleLength = tupleLength(0).split(",").size + 1
    val itemsets = L_kMinusOne.map{ case (str, sup) => str.replaceAll("[\\[()\\]]", "") }
    //Collect and parallellise
    val cartesianProd = itemsets.cartesian(freqItems_L1)
    //cartesianProd.foreach(println)
    val freqItemCombos = cartesianProd.map(x=>x.toString.replaceAll("[\\[()\\]]", "").split(",").toSet.toSeq.sorted)
      .filter(tupleSet => tupleSet.size == desiredTupleLength)
      .map(strArr => strArr.map(_.toInt)).collect.toSet
    val freqItemCombosStr = freqItemCombos.map(setOfInts => setOfInts.mkString(","))
    val freqItemCombosRdd: org.apache.spark.rdd.RDD[String] = sc.parallelize(freqItemCombosStr.toSeq)
    return freqItemCombosRdd
  }

  def generateL_k(transactions: org.apache.spark.rdd.RDD[String], freqCombos: org.apache.spark.rdd.RDD[String], support: Int, k: Int)
  : org.apache.spark.rdd.RDD[(String, Int)]
  =
  {
    //Delay 1
    if(addDelay&&k%2 == 0){
      Thread.sleep(2000)
    }
    val itemsPerTxn = transactions.map(txn => txn.split(" "))
    //Delay 2
    val itemsPerTxnInt = itemsPerTxn.map(txnItems =>{ if(addDelay&&(txnItems.contains("10")||txnItems.contains("20"))){
      Thread.sleep(100)
      System.out.println("Waiting 100ms on transaction "+txnItems.toString)
    }
      txnItems.map(_.toInt)})
    val txnCombos = itemsPerTxnInt.map(txnItems => { txnItems.combinations(k).toArray})
    val txnCombosStr = txnCombos.map(arrOfarr => arrOfarr.map(arrOfInt => arrOfInt.toSet.mkString(",")))
    val freqCombosSets = freqCombos.collect().toSet
    val intersection = txnCombosStr.map(arr => arr.filter(tuple => freqCombosSets.contains(tuple)))
    val intersectionOnes = intersection.flatMap(arrayOfPairs => arrayOfPairs.map(array => ("["+array+"]",1)))
    val C_k = intersectionOnes.reduceByKey(_+_)
    val L_k = C_k.filter{case (str,sup) => sup >= support}
    println("--- --- --- --- ---\n")
    if(L_k.isEmpty())
    {
      print("No more frequent itemsets.")
    }
    else
    {
      println("k = " + k)
      println("\nFrequent itemsets:")
      L_k.foreach(println)
    }

    return L_k
  }
}