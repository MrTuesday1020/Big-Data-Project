package comp9313.proj2

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

object Problem1 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFolder = args(1)
    val conf = new SparkConf().setAppName("Problem1").setMaster("local")
    val sc = new SparkContext(conf)
    val input =  sc.textFile(inputFile)

    // split lines
    val lines = input.flatMap(line => line.split("\n"))
	  // split words
    val wordsets = lines.map(words => words.toLowerCase().split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"))
    
	  // Map
    val cooccurances = new ListBuffer[(String, String, Int)]
    wordsets.collect().foreach { wordset => 
      {
        for(i <- 0 to wordset.length - 1 ; j <- i + 1 to wordset.length-1){
          if (wordset(i).matches("""^[a-z]+$""") && wordset(j).matches("""^[a-z]+$""")){
            cooccurances += ((wordset(i), wordset(j), 1))
          }
        }
      }  
    }
    
	  // Reduce    
    val Nmap = sc.parallelize(cooccurances).map{case(k1,k2,v) => ((k1,k2),(v))}.reduceByKey(_+_).collect().toMap
    val SumNmap = sc.parallelize(cooccurances).map{case(k1,k2,v) => ((k1),(v))}.reduceByKey(_+_).collect().toMap
    
	  // calculate results
    val results = new ListBuffer[(String, String, Double)]
    Nmap.foreach{ record => 
      {
        val k1 = record._1._1
        val k2 = record._1._2
        val N = record._2
        val SumN = SumNmap(k1)
        val f = N / SumN.toDouble
        results += ((k1, k2, f))
      }
    }
    
	  // sort results
    val sorted_results = results.sortBy{case(k1,k2,v) => (k1,-v,k2)}
    
	  //format output
    val output = sc.parallelize(sorted_results).map{ case(k1,k2,v) => (k1+" "+k2+" "+v) }
	
	  //output    
	  output.saveAsTextFile(outputFolder)
  }
}