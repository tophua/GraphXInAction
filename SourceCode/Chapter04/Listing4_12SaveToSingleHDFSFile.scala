package org.apache.spark.examples.graphx


import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
object saveToSingleHDFSFile{
  def main(args: Array[String]) {
    val conf = new org.apache.hadoop.conf.Configuration
    conf.set("fs.defaultFS", "hdfs://localhost")
    val fs = FileSystem.get(conf)
    FileUtil.copyMerge(fs, new Path("/user/cloudera/myGraphVertices/"),
      fs, new Path("/user/cloudera/myGraphVerticesFile"), false, conf, null)
  }
}

