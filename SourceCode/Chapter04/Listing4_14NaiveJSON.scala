
package Chapter04.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
object naiveJSON{
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setMaster("local")
          .setAppName("helloworld"))



        val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
            (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

        val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
            Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
            Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

        val myGraph = Graph(myVertices, myEdges)
        myGraph.vertices.map(x => {
            val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
            mapper.registerModule(
                com.fasterxml.jackson.module.scala.DefaultScalaModule)
            val writer = new java.io.StringWriter()
            mapper.writeValue(writer, x)
            writer.toString
        }).coalesce(1,true).saveAsTextFile("myGraphVertices")


        sc.stop
    }
}
