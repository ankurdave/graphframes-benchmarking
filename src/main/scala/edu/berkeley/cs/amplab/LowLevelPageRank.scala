package edu.berkeley.cs.amplab

import scala.collection.mutable.ArrayBuilder
import scala.collection.mutable.HashMap

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.util.MutablePair

object LowLevelPageRank {
  def main(args: Array[String]): Unit = {
    val iters = args(0).toInt
    val file = args(1)

    val conf = new SparkConf().setAppName("LowLevelPageRank")
    val sc = new SparkContext(conf)

    val rawEdges = sc.textFile(file)
    val numEdgePartitions = rawEdges.partitions.length
    val edgePartitioner = new HashPartitioner(numEdgePartitions)
    val edges = rawEdges
      .mapPartitionsWithIndex { (pid, lines) =>
        val srcIds = ArrayBuilder.make[Long]
        val dstIds = ArrayBuilder.make[Long]
        for (line <- lines) {
          if (!line.startsWith("#")) {
            val fields = line.split('\t')
            srcIds += fields(0).toLong
            dstIds += fields(1).toLong
          }
        }
        val srcIdsArr = srcIds.result
        val dstIdsArr = dstIds.result
        new Sorter(sdf).sort((srcIdsArr, dstIdsArr), 0, srcIdsArr.size, ord)
        val dataArr = Array.fill(srcIdsArr.size) { 1.0 }
        Iterator((pid, (srcIdsArr, dstIdsArr, dataArr)))
      }.partitionBy(edgePartitioner)
    time("build edges") {
      edges.setName("edges").cache().count
    }

    val vertexPartitioner = new HashPartitioner(200)
    val routingTables = edges.mapPartitions(_.flatMap {
      case (pid, (srcIds, dstIds, data)) =>
        RoutingTablePartition.edgePartitionToMsgs(pid, srcIds, dstIds)
    }).partitionBy(vertexPartitioner).mapPartitions(
      iter => Iterator(RoutingTablePartition.fromMsgs(numEdgePartitions, iter)),
      preservesPartitioning = true)
    time("build routing tables") {
      routingTables.setName("routingTables").cache().count
    }

    val vertices = routingTables.mapPartitions({ routingTableIter =>
      val routingTable =
        if (routingTableIter.hasNext) routingTableIter.next() else RoutingTablePartition.empty
      val map = HashMap.empty[Long, Int]
      val attrs = ArrayBuilder.make[Double]
      var i = 0
      for (id <- routingTable.iterator) {
        if (!map.contains(id)) {
          map.update(id, i)
          attrs += 1.0
          i += 1
        }
      }
      Iterator((map, attrs.result))
    }, preservesPartitioning = true)
    time("build vertices") {
      vertices.setName("vertices").cache().count
    }

    var ranks = vertices

    for (i <- 1 to iters) {
      val shippedRanks =
        ranks.zipPartitions(routingTables, preservesPartitioning = true) {
          (vIter, rtIter) => vIter.flatMap {
            case (map, attrs) => rtIter.flatMap { rt =>
              Iterator.tabulate(rt.numEdgePartitions) { pid =>
                val blockIds = ArrayBuilder.make[Long]
                val blockAttrs = ArrayBuilder.make[Double]
                rt.foreachWithinEdgePartition(pid, true, false) { id =>
                  blockIds += id
                  blockAttrs += attrs(map(id))
                  ()
                }
                (pid, new VertexAttributeBlock(blockIds.result, blockAttrs.result))
              }
            }
          }
        }.partitionBy(edgePartitioner)
      val newRanks = edges.zipPartitions(shippedRanks) {
        (ePartIter, shippedVertBlockIter) => ePartIter.flatMap {
          case (pid, (srcIds, dstIds, data)) =>
            val ranks = HashMap.empty[Long, Double]
            val contribs = HashMap.empty[Long, Double]
            shippedVertBlockIter.foreach {
              case (_, shippedVertBlock) =>
                var i = 0
                while (i < shippedVertBlock.ids.size) {
                  ranks.update(shippedVertBlock.ids(i), shippedVertBlock.attrs(i))
                  i += 1
                }
            }
            var i = 0
            while (i < srcIds.size) {
              val contrib = srcIds(i) * data(i)
              if (contribs.contains(dstIds(i))) {
                contribs(dstIds(i)) += contrib
              } else {
                contribs(dstIds(i)) = contrib
              }
              i += 1
            }
            contribs.iterator
        }
      }.reduceByKey(vertexPartitioner, _ + _).zipPartitions(ranks, preservesPartitioning = true) {
        (contribIter, rankPartIter) => rankPartIter.map {
          case (map, attrs) =>
            val newAttrs = new Array[Double](attrs.size)
            contribIter.foreach {
              case (id, contrib) =>
                newAttrs(map(id)) = 0.15 + 0.85 * contrib
            }
            (map, newAttrs)
        }
      }
      time("iteration " + i) {
        newRanks.setName("ranks after iteration " + i).cache().count
      }
      ranks.unpersist()
      ranks = newRanks
    }

    ranks.count
    ()
  }

  def ord = new Ordering[MutablePair[Long, Long]] {
    override def compare(x: MutablePair[Long, Long], y: MutablePair[Long, Long]): Int = {
      if (x._1 == y._1) {
        if (x._2 == y._2) 0
        else if (x._2 < y._2) -1
        else 1
      } else if (x._1 < y._1) -1
      else 1
    }
  }

  def sdf = new SortDataFormat[MutablePair[Long, Long], (Array[Long], Array[Long])] {
    override def newKey(): MutablePair[Long, Long] = MutablePair(0L, 0L)
    override protected def getKey(
        data: (Array[Long], Array[Long]), pos: Int): MutablePair[Long, Long] = {
      getKey(data, pos, newKey())
    }
    override def getKey(
        data: (Array[Long], Array[Long]), pos: Int,
        reuse: MutablePair[Long, Long]): MutablePair[Long, Long] = {
      reuse._1 = data._1(pos)
      reuse._2 = data._2(pos)
      reuse
    }
    override def swap(data: (Array[Long], Array[Long]), pos0: Int, pos1: Int): Unit = {
      val tmp1 = data._1(pos0)
      val tmp2 = data._2(pos0)
      data._1(pos0) = data._1(pos1)
      data._2(pos0) = data._2(pos1)
      data._1(pos1) = tmp1
      data._2(pos1) = tmp2
    }
    override def copyElement(
        src: (Array[Long], Array[Long]), srcPos: Int,
        dst: (Array[Long], Array[Long]), dstPos: Int): Unit = {
      dst._1(dstPos) = src._1(srcPos)
      dst._2(dstPos) = src._2(srcPos)
    }
    override def copyRange(
        src: (Array[Long], Array[Long]), srcPos: Int,
        dst: (Array[Long], Array[Long]), dstPos: Int, length: Int): Unit = {
      System.arraycopy(src._1, srcPos, dst._1, dstPos, length)
      System.arraycopy(src._2, srcPos, dst._2, dstPos, length)
    }
    override def allocate(length: Int): (Array[Long], Array[Long]) = {
      (new Array[Long](length), new Array[Long](length))
    }
  }

  def time[A](desc: String)(f: => A): A = {
    val start = System.nanoTime
    val result = f
    println(s"$desc: ${(System.nanoTime - start) / 1000000.0} ms")
    result
  }
}

/** Stores vertex attributes to ship to an edge partition. */
class VertexAttributeBlock(val ids: Array[Long], val attrs: Array[Double])
  extends Serializable {
  def iterator: Iterator[(Long, Double)] =
    (0 until ids.size).iterator.map { i => (ids(i), attrs(i)) }
}
