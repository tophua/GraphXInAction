package org.apache.spark.examples.graphx
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object personalizedPageRank{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("helloworld"))

    val g = GraphLoader.edgeListFile(sc, "cit-HepTh.txt")
    g.personalizedPageRank(9207016, 0.001)
      .vertices
      .filter(_._1 != 9207016)
      .reduce((a,b) => if (a._2 > b._2) a else b)
    sc.stop
  }
}

