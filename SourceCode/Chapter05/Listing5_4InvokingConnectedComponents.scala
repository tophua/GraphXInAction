package org.apache.spark.examples.graphx
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object invokingConnectedComponents{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("helloworld"))
    val g = Graph(sc.makeRDD((1L to 7L).map((_,""))),
      sc.makeRDD(Array(Edge(2L,5L,""), Edge(5L,3L,""), Edge(3L,2L,""),
        Edge(4L,5L,""), Edge(6L,7L,"")))).cache
    g.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).collect
    sc.stop
  }
}



