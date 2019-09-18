package com.commons

/**
 * 常量接口
 */
object Constants {

	/**
	 * 项目配置相关的常量
	 */
	val JDBC_DATASOURCE_SIZE = "jdbc.datasource.size"
	val JDBC_URL = "jdbc.url"
	val JDBC_USER = "jdbc.user"
	val JDBC_PASSWORD = "jdbc.password"

	val KAFKA_TOPICS = "kafka.topics"
	
	/**
	 * Spark作业相关的常量
	 */
	val SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark"
	val SPARK_APP_NAME_PAGE = "PageOneStepConvertRateSpark"

}
