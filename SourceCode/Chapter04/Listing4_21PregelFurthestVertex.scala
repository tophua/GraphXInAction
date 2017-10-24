package Chapter04.demo

import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}
object pregelFurthestVertex{
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setMaster("local")
      .setAppName("helloworld"))



    val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
      (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

    val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
      Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
      Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

    val myGraph = Graph(myVertices, myEdges)

    val g = Pregel(myGraph.mapVertices((vid,vd) => 0), 0,
      activeDirection = EdgeDirection.Out)(
      (id:VertexId,vd:Int,a:Int) => math.max(vd,a),
      (et:EdgeTriplet[Int,String]) =>
        Iterator((et.dstId, et.srcAttr+1)),
      (a:Int,b:Int) => math.max(a,b))
    g.vertices.collect
    sc.stop
  }
}

