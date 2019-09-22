package com.analysis.tagging

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.TagUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 9:51
  * @description:
  * @since 1.0
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath, docs, stopwords) = args

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


    val srcDateFarme: DataFrame = sparkSession.read.parquet(inputPath)

    val docMap: collection.Map[String, String] = sparkSession.sparkContext.textFile(docs).map(_.split("\\s")).filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()

    val broadValue: Broadcast[collection.Map[String, String]] = sparkSession.sparkContext.broadcast(docMap)

    // 读取停用词典
    val stopwordsMap = sparkSession.sparkContext.textFile(stopwords).map((_, 0)).collectAsMap()
    // 广播字典
    val broadValues = sparkSession.sparkContext.broadcast(stopwordsMap)

    //    srcDateFarme.createOrReplaceTempView("log")

    val userId2BusinessTagRDD: RDD[(String, List[(String, Int)])] = srcDateFarme.map {
      case item =>
        // 获取用户唯一ID
        val userId = TagUtils.getOneUserId(item)
        // 广告标签
        val adList: List[(String, Int)] = TagsAd.makeTags(item)
        // 商圈
        // val bussList: List[(String, Int)] = BusinessTag.makeTags(item)
        // 媒体标签
        val appList: List[(String, Int)] = TagsAPP.makeTags(item, broadValue)
        // 设备标签
        val devList: List[(String, Int)] = TagsDevice.makeTags(item)
        // 地域标签
        val locList: List[(String, Int)] = TagsLocation.makeTags(item)
        // 关键字标签
        val kwList: List[(String, Int)] = TagsKword.makeTags(item, broadValues)

        //        (userId, adList ++ bussList ++ appList ++ devList ++ locList ++ kwList )
        (userId, adList ++ appList ++ devList ++ locList ++ kwList)
    }.rdd.reduceByKey {
      case (list1, list2) => {
        (list1 ::: list2).groupBy(_._1)
          .mapValues(_.foldLeft(0)(_ + _._2))
          .toList
      }
    }

    userId2BusinessTagRDD.foreach(println(_))

    sparkSession.stop()

  }
}

case class UserTag(userId: String, BTag: String)

/*
val userId2FilterBTagRDD: RDD[(String, List[(String, Int)])] = userId2BusinessTagRDD.filter {
      case (userId, listBusinessTag) =>
        listBusinessTag.size > 0
    }

    val userTagRDD: RDD[UserTag] = userId2FilterBTagRDD.map {
      case (userId, listBTag) =>
        var aggInfo: String = ""
        for (item <- listBTag) {
          val name = item._1
          val count = item._2
          aggInfo += name + "=" + count + "|"
        }
        if (aggInfo.endsWith("\\|")) {
          aggInfo = aggInfo.substring(0, aggInfo.length - 1)
        }
        UserTag(userId, aggInfo)
    }

    userTagRDD.foreach(println(_))

    println("===========ok===========")
 */