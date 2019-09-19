package com.utils

/**
  *
  * @author: ZDC
  * @date: 2019/09/19 10:19
  * @description:
  * @since 1.0
  */
trait Tag {

  def makeTags(args:Any*):List[(String, Int)]

}
