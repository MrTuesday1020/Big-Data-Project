package comp9313.proj3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.math.BigDecimal

object SetSimJoin {
  def prefix_filter(rdd:RDD[(Int, Array[Int])], threshold:Double): RDD[(Int, Array[Int])] = {
     rdd.flatMap{ line =>
       // assign record id
       val record_id = line._1
       // remove record id from list
       val elements = line._2.sorted
       // get the length of the map
       val length = elements.length
       // prefix filter principle
       val index = (length - Math.ceil(threshold * length) + 1).toInt
       // prefix set
       val prefix = elements.slice(0, index)
       prefix.map { x => (x,  Array((record_id))) }
    }
  }

  def main(args: Array[String]) {
    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputFolder = args(2)
    val threshold = args(3).toDouble
    val conf = new SparkConf().setAppName("SetSimJoin").setMaster("local")
    val sc = new SparkContext(conf)
    // format: ((record_id1, elements_list1), (record_id2, elements_list2), (record_id3, elements_list3)....)
    val rdd1 = sc.textFile(inputFile1).map { line => line.split(" ") }.map { line => (line(0).toInt, line.drop(1).map { element => element.toInt}) }
    val rdd2 = sc.textFile(inputFile2).map { line => line.split(" ") }.map { line => (line(0).toInt, line.drop(1).map { element => element.toInt}) }
    
    val dict1 = rdd1.collectAsMap()
    val dict2 = rdd2.collectAsMap()
    
    // map
    val map1 = prefix_filter(rdd1, threshold)
    val map2 = prefix_filter(rdd2, threshold)
    
    // reduce
    val reduce1 = map1.reduceByKey(_++_)
    val reduce2 = map2.reduceByKey(_++_)
    
    // combine two maps Array((1,2),(1,3))
    val combined = reduce1 ++ reduce2
    // group by key Map(1, Array(1,2),(1,3))
    val grouped = combined.groupBy(_._1)
    // transform Map(1, Array(1,2),(1,3)) to Map(1, List(2,3))
    val transform = grouped.mapValues(_.map(_._2).toList)
    // remove the elements appearing in only one file
    val cleaned = transform.filter(f => f._2.length == 2)
    
    val pairs = cleaned.map{ line => 
      val f1 = line._2(0)
      val f2 = line._2(1)
      // combination
      for{
        x <- f1
        y <- f2
        // length filter
        if( Math.min(dict1(x).length, dict2(y).length).toDouble / Math.max(dict1(x).length, dict2(y).length).toDouble >= threshold )
      } yield (x, y)
    }.flatMap( x => x ).distinct()
    
    val similarities = pairs.map{ x => 
      val record_id1 = x._1
      val record_id2 = x._2
      val intersection = dict1(record_id1).toSet & dict2(record_id2).toSet
      val union = dict1(record_id1).toSet ++ dict2(record_id2).toSet
      val similarity = BigDecimal(intersection.size.toDouble / union.size.toDouble).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
      (record_id1, record_id2, similarity)
    }.filter( f => f._3 >= threshold)
    
    // sort results
    val sorted_results = similarities.sortBy{ case(id1, id2, sim) => (id1, id2, sim) }
     //format output
    val output = sorted_results.map{ case(id1, id2, sim) => s"($id1,$id2)\t$sim" }
	  //output    
	  output.saveAsTextFile(outputFolder)

  }
}