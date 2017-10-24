package org.apache.spark.examples.graphx
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

object triangleCountsOnSlashdotFriendAndFoeData{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("helloworld"))

    val g = GraphLoader.edgeListFile(sc, "soc-Slashdot0811.txt").cache
    val g2 = Graph(g.vertices, g.edges.map(e =>
      if (e.srcId < e.dstId) e else new Edge(e.dstId, e.srcId, e.attr))).
      partitionBy(PartitionStrategy.RandomVertexCut)
    (0 to 6).map(i => g2.subgraph(vpred =
      (vid,_) => vid >= i*10000 && vid < (i+1)*10000).
      triangleCount.vertices.map(_._2).reduce(_ + _))
    //lib.ShortestPaths.run(myGraph,Array(3)).vertices.collect
    sc.stop
  }
}

