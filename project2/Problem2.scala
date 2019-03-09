package comp9313.proj2

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

object Problem2 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)
    
    val fileName = args(0)
    val k = args(1).toInt
    
	// build graph
	val edges = sc.textFile(fileName)   
    val edgelist = edges.map(x => x.split(" ")).map(x=> Edge(x(1).toLong, x(2).toLong, 1.0))
    val graph = Graph.fromEdges[Double, Double](edgelist, 1.0)
    
	// init graph
    val initialGraph = graph.mapVertices((id, _) => Set[String](""))

	// do pregel iteration and find all the paths
    val res = initialGraph.pregel(initialMsg = Set[String](""), maxIterations = k)(
        // Vertex Program
        (id, attr, msg) => attr ++ msg ,
        // Send Message
        triplet => {
            Iterator((triplet.dstId, triplet.srcAttr.map { x => x + triplet.srcId.toString() + "-" }))
        },
        //Merge Message
      (a, b) => a ++ b
    )

	// find the number of cycles according paths
    val temp1 = res.vertices.map{case(id,setstring) => setstring.map { newsetstring => newsetstring + id.toString() }}
    val temp2 = temp1.map { x => x.map { y => y.split("-").toList } } 
    val results = ListBuffer[String]()
    temp2.collect().foreach{ x=>x.map { y => 
        if(y.length == k+1){
          if(y(0) == y(k)){
            if(y.distinct.length == k){
              val sortedy = y.distinct.sortWith(_<_).toString()
              results += sortedy
            }
          }
        } 
      }
    }
    
    println(results.distinct.length)
  }
}