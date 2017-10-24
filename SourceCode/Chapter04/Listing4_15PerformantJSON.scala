import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
object performantJSON{
    def main(args: Array[String]) {
        val sc = new SparkContext(new SparkConf().setMaster("local")
          .setAppName("helloworld"))



        val myVertices = sc.makeRDD(Array((1L, "Ann"), (2L, "Bill"),
            (3L, "Charles"), (4L, "Diane"), (5L, "Went to gym this morning")))

        val myEdges = sc.makeRDD(Array(Edge(1L, 2L, "is-friends-with"),
            Edge(2L, 3L, "is-friends-with"), Edge(3L, 4L, "is-friends-with"),
            Edge(4L, 5L, "Likes-status"), Edge(3L, 5L, "Wrote-status")))

        val myGraph = Graph(myVertices, myEdges)

        myGraph.vertices.mapPartitions(vertices => {
            val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
            mapper.registerModule(DefaultScalaModule)
            val writer = new java.io.StringWriter()
            vertices.map(v => {writer.getBuffer.setLength(0)
                mapper.writeValue(writer, v)
                writer.toString})
        }).coalesce(1,true).saveAsTextFile("myGraphVertices")

        myGraph.edges.mapPartitions(edges => {
            val mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.registerModule(DefaultScalaModule)
            val writer = new java.io.StringWriter()
            edges.map(e => {writer.getBuffer.setLength(0)
                mapper.writeValue(writer, e)
                writer.toString})
        }).coalesce(1,true).saveAsTextFile("myGraphEdges")

        val myGraph2 = Graph(
            sc.textFile("myGraphVertices").mapPartitions(vertices => {
                val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                vertices.map(v => {
                    val r = mapper.readValue[Tuple2[Integer,String]](v,
                        new TypeReference[Tuple2[Integer,String]]{})
                    (r._1.toLong, r._2)
                })
            }),
            sc.textFile("myGraphEdges").mapPartitions(edges => {
                val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                edges.map(e => mapper.readValue[Edge[String]](e,
                    new TypeReference[Edge[String]]{}))
            })
        )

        sc.stop
    }
}
