package org.apache.spark.examples.graphx
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object pageRankDemo {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("helloworld"))
    val graph = GraphLoader.edgeListFile(sc, "cit-HepTh.txt")
    val v = graph.pageRank(0.001).vertices
    v.reduce((a,b) => if (a._2 > b._2) a else b)
    sc.stop
  }
}


