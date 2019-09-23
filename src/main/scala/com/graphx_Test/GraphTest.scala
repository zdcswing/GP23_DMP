package com.graphx_Test

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * @author: ZDC
  * @date: 2019/09/23 10:35
  * @description:
  * @since 1.0
  */
object GraphTest {

  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder().master("local[*]").appName("").getOrCreate()


    // 创建点和边
    // 构建点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sparkSession.sparkContext.makeRDD(Seq(
      (1L, ("小1", 26)),
      (2L, ("小2", 30)),
      (6L, ("小3", 33)),
      (9L, ("小4", 26)),
      (138L, ("小5", 30)),
      (138L, ("小6", 33)),
      (158L, ("小7", 26)),
      (16L, ("小8", 30)),
      (44L, ("小9", 33)),
      (21L, ("小10", 26)),
      (5L, ("小11", 30)),
      (7L, ("小12", 33))
    ))

    // 构建边的集合
    val edgeRDD: RDD[Edge[Int]] = sparkSession.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(11L, 133L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    // 构建图
    val graph = Graph(vertexRDD, edgeRDD)

    // 取顶点
    val vertexs: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 匹配数据
//    vertexs.join(vertexs).map{
////      case (userId,(cnId, (name, age)))=>(cnId)
//    }

  }

}
