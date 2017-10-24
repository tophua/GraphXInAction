package org.apache.spark.examples.graphx


import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object inDegreesDemo {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("helloworld"))


    val graph = GraphLoader.edgeListFile(sc, "cit-HepTh.txt")
    graph.inDegrees.reduce((a,b) => if (a._2 > b._2) a else b)

    sc.stop
  }
}

