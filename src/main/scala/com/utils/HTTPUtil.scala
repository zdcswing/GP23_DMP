package com.utils


import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 10:32
  * @description:
  * @since 1.0
  */
object HTTPUtil {

  /**
    * get 请求
    * @param url
    * @return json
    */
  def get(url: String): String = {

    val client: CloseableHttpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)

    // 发送请求
    val httpResponse: CloseableHttpResponse = client.execute(httpGet)

    // 处理请求返回结果
    EntityUtils.toString(httpResponse.getEntity, "UTF8")

  }
}
