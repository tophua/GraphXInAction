package Chapter04.demo

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.{SparkConf, SparkContext}
object fileRoundTrip{
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setMaster("local")
          .setAppName("helloworld"))



        val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
            (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

        val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
            Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
            Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

        val myGraph = Graph(myVertices, myEdges)

        myGraph.vertices.saveAsObjectFile("myGraphVertices")
        myGraph.edges.saveAsObjectFile("myGraphEdges")
        val myGraph2 = Graph(
            sc.objectFile[Tuple2[VertexId,String]]("myGraphVertices"),
            sc.objectFile[Edge[String]]("myGraphEdges"))
        sc.stop
    }
}


