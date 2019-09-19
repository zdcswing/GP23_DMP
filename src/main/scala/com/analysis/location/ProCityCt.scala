package com.analysis.location

import com.commons.{ConfigurationManager, Constants}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  * @author: ZDC
  * @date: 2019/09/17 15:58
  * @description:
  * @since 1.0
  */
object ProCityCt {


  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("输入目录不正确")
      sys.exit()
    }

    val Array(inputPath) = args

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("ct")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 获取数据
    val srcDataFrame: DataFrame = sparkSession.read.parquet(inputPath)

    df2ProCityBySql(sparkSession, srcDataFrame)
    df2ProCityByApi(sparkSession, srcDataFrame)


  }

  def df2ProCityByApi(sparkSession: SparkSession, srcDF: DataFrame): Unit = {

    import org.apache.spark.sql.functions._
    val aggDF: DataFrame = srcDF.groupBy(col("provincename"), col("cityname"))
      .agg(count("cityname") as "ct")

    aggDF.write.save("D:\\out\\procity")

  }


  def df2ProCityBySql(sparkSession: SparkSession, srcDF: DataFrame): Unit = {
    // 注册临时视图
    srcDF.createOrReplaceTempView("log")

    val sql = "select provincename, cityname, count(*) ct from log group by provincename, cityname"

    // 查询结果
    val df2: DataFrame = sparkSession.sql(sql)
    //
    //    df2.write.partitionBy("provincename", "cityname").json("d:\\out\\procity")
    df2.foreach(println(_))
    // 存入mysql
    val load = ConfigFactory.load()
    df2.write.format("jdbc")
      .option("url", load.getString(Constants.JDBC_URL))
      .option("user", load.getString(Constants.JDBC_USER))
      .option("password", load.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "procity")
      .mode(SaveMode.Append)
      .save()

    //df2.write.format("jdbc")
    //  .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
    //  .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
    //  .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
    //  .option("dbtable", "procity")
    //  .mode(SaveMode.Append)
    //  .save()

  }


}
