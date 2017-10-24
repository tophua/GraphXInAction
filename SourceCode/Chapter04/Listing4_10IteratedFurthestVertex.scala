package Chapter04.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.rdd.RDD
object iteratedFurthestVertex{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("helloworld"))



    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)

    myGraph.aggregateMessages[Int](_.sendToSrc(1),
      _ + _).rightOuterJoin(myGraph.vertices).map(_._2.swap).collect


    sc.stop
  }

  def sendMsg(ec: EdgeContext[Int,String,Int]): Unit = {
    ec.sendToDst(ec.srcAttr+1)
  }

  def mergeMsg(a: Int, b: Int): Int = {
    math.max(a,b)
  }

  def propagateEdgeCount(g:Graph[Int,String]):Graph[Int,String] = {
    val verts = g.aggregateMessages[Int](sendMsg, mergeMsg)
    val g2 = Graph(verts, g.edges)
    val check = g2.vertices.join(g.vertices).
      map(x => x._2._1 - x._2._2).
      reduce(_ + _)
    if (check > 0)
      propagateEdgeCount(g2)
    else
      g
  }
}

