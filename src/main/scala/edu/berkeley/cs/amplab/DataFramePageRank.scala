package edu.berkeley.cs.amplab

import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.HashMap

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.util.MutablePair
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object DataFramePageRank {
  def main(args: Array[String]): Unit = {
    val iters = args(0).toInt
    val file = args(1)

    val conf = new SparkConf().setAppName("LowLevelPageRank")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rawEdges = sc.textFile(file)
    val numEdgePartitions = rawEdges.partitions.length
    val edgePartitioner = new HashPartitioner(numEdgePartitions)
    val edges = rawEdges
      .flatMap { line =>
          if (!line.startsWith("#")) {
            val fields = line.split('\t')
            Some((fields(0).toLong, fields(1).toLong, 1.0))
          } else {
            None
          }
      }.toDF("src_id", "dst_id", "weight")
      .repartition($"src_id")//.sortWithinPartitions($"src_id", $"dst_id")
    time("build edges") {
      edges.cache().count
    }

    val vertices = edges.explode($"src_id", $"dst_id") {
      case Row(srcId: Long, dstId: Long) => Seq(VertexId(srcId), VertexId(dstId))
    }.distinct().select($"id", lit(1.0).as("rank")).repartition($"id")
    time("build vertices") {
      vertices.cache().count
    }

    var ranks = vertices

    for (i <- 1 to iters) {
      val newRanks = edges.join(ranks, $"src_id" === $"id")
        .select($"dst_id", ($"weight" * $"rank").as("contrib"))
        .groupBy($"dst_id")
        .agg(sum("contrib").as("totalContrib"))
        .select($"dst_id".as("id"), (lit(0.15) + lit(0.85) * $"totalContrib").as("rank"))

      newRanks.explain(true)

      time("iteration " + i) {
        newRanks.cache().count
      }
      ranks.unpersist()
      ranks = newRanks
    }

    ranks.count
    ()
  }

  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    result
  }
}

case class VertexId(id: Long)
