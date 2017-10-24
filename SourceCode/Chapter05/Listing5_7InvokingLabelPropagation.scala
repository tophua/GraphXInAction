package org.apache.spark.examples.graphx


import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object invokingLabelPropagation{
 def main(args: Array[String]) {
  val sc = new SparkContext(new SparkConf().setMaster("local")
    .setAppName("helloworld"))



  val v = sc.makeRDD(Array((1L,""), (2L,""), (3L,""), (4L,""), (5L,""),
   (6L,""), (7L,""), (8L,"")))
  val e = sc.makeRDD(Array(Edge(1L,2L,""), Edge(2L,3L,""), Edge(3L,4L,""),
   Edge(4L,1L,""), Edge(1L,3L,""), Edge(2L,4L,""), Edge(4L,5L,""),
   Edge(5L,6L,""), Edge(6L,7L,""), Edge(7L,8L,""), Edge(8L,5L,""),
   Edge(5L,7L,""), Edge(6L,8L,"")))
  lib.LabelPropagation.run(Graph(v,e),5).vertices.collect.
    sortWith(_._1<_._1)

  sc.stop
 }
}
