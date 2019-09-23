package com.analysis.tagging

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.TagUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 9:51
  * @description:
  * @since 1.0
  */
object TagsContext2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath, docs, stopwords, day) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("TagsContext")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import sparkSession.implicits._

    val load: Config = ConfigFactory.load()

    val hbaseTableName: String = load.getString("HBASE.tableName")

    val configuration: Configuration = sparkSession.sparkContext.hadoopConfiguration

    configuration.set("", load.getString("HBASE.Host"))

    val hbConn: Connection = ConnectionFactory.createConnection(configuration)

    val hAdmin: Admin = hbConn.getAdmin

    if (!hAdmin.tableExists(TableName.valueOf(hbaseTableName))) {
      println("当前表有效")
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val hColumnDescriptor = new HColumnDescriptor("tags")

      tableDescriptor.addFamily(hColumnDescriptor)
      hAdmin.createTable(tableDescriptor)
      hAdmin.close()
      hbConn.close()
    }

    val conf = new JobConf(configuration)
    // 指定输出类型
    conf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出哪张表
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)


    val srcDateFarme: DataFrame = sparkSession.read.parquet(inputPath)

    val docMap: collection.Map[String, String] = sparkSession.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    val broadValue: Broadcast[collection.Map[String, String]] = sparkSession.sparkContext.broadcast(docMap)

    // 读取停用词典
    val stopwordsMap = sparkSession.sparkContext.textFile(stopwords).map((_, 0)).collectAsMap()
    // 广播字典
    val broadValues = sparkSession.sparkContext.broadcast(stopwordsMap)

    //    srcDateFarme.createOrReplaceTempView("log")

    val allUserId: RDD[(List[String], Row)] = srcDateFarme.rdd.map(row => {
      val strList = TagUtils.getallUserId(row)
      (strList, row)
    })

    // 构建点集合
    val verties: RDD[(Long, List[(String, Int)])] = allUserId.flatMap {
      case (strList, row) =>

        // 广告标签
        val adList: List[(String, Int)] = TagsAd.makeTags(row)
        // 商圈
        // val bussList: List[(String, Int)] = BusinessTag.makeTags(row)
        // 媒体标签
        val appList: List[(String, Int)] = TagsAPP.makeTags(row, broadValue)
        // 设备标签
        val devList: List[(String, Int)] = TagsDevice.makeTags(row)
        // 地域标签
        val locList: List[(String, Int)] = TagsLocation.makeTags(row)
        // 关键字标签
        val kwList: List[(String, Int)] = TagsKword.makeTags(row, broadValues)
        // 获取所有的标签
        val tagList = adList ++ appList ++ devList ++ locList ++ kwList
        // 保留用户Id
        val VD = strList.map((_, 0)) ++ tagList
        // 思考 1. 如何保证其中一个ID携带着用户的标签
        //      2. 用户ID的字符串如何处理
        strList.map {
          case uId =>
            if (strList.head.equals(uId)) {
              (uId.hashCode.toLong, VD)
            }
            else {
              (uId.hashCode.toLong, List.empty)
            }
        }
    }

    // 构建边
    val edges: RDD[Edge[Int]] = allUserId.flatMap {
      case (strList, row) =>
        strList.map(uId => Edge(uId.head.hashCode().toLong, uId.hashCode().toLong, 0))
    }

    // 构建图
    val graph = Graph(verties, edges)

    // 根据图计算中的连通图算法, 通过图中的分支, 连通所有的点
    // 然后再根据所有点, 找到内部最小的点, 为当前的公共点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    // 聚合所有标签
    vertices.join(verties).map {
      case (uid, (cnId, tagsAndUserId)) =>
        (cnId, tagsAndUserId)
    }.reduceByKey((list1, list2) => {
      (list1 ++ list2).groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
    }).map{
      case (userId, userTags) =>{
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
        (new ImmutableBytesWritable(), put)
      }
    }.saveAsHadoopDataset(conf)


    sparkSession.stop()

  }
}
