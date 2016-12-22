package com.knoldus.spark.graphx.example

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FindInfluencer {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Twittter Influencer").setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("ERROR")

    val twitterData = sparkContext.textFile("src/main/resources/twitter-graph-data.txt")

    val followeeVertices: RDD[(VertexId, String)] = twitterData.map(_.split(",")).map { arr =>
      val user = arr(0).replace("((", "")
      val id = arr(1).replace(")", "")
      (id.toLong, user)
    }

    val followerVertices: RDD[(VertexId, String)] = twitterData.map(_.split(",")).map { arr =>
      val user = arr(2).replace("(", "")
      val id = arr(3).replace("))", "")
      (id.toLong, user)
    }

    val vertices = followeeVertices.union(followerVertices)
    val edges: RDD[Edge[String]] = twitterData.map(_.split(",")).map { arr =>
      val followeeId = arr(1).replace(")", "").toLong
      val followerId = arr(3).replace("))", "").toLong
      Edge(followeeId, followerId, "follow")
    }

    val defaultUser = ("")
    val graph = Graph(vertices, edges, defaultUser)

    val subGraph = graph.pregel("", 3, EdgeDirection.In)((_, attr, msg) =>
      attr + "," + msg,
      triplet => Iterator((triplet.srcId, triplet.dstAttr)),
      (a, b) => (a + "," + b))

    val lengthRDD = subGraph.vertices.map(vertex => (vertex._1, vertex._2.split(",").distinct.length - 2)).max()(new Ordering[Tuple2[VertexId, Int]]() {
      override def compare(x: (VertexId, Int), y: (VertexId, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })

    val userId = graph.vertices.filter(_._1 == lengthRDD._1).map(_._2).collect().head
    println(userId + " has maximum influence on network with " + lengthRDD._2 + " influencers.")

    sparkContext.stop()
  }
}
