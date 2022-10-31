package com.cqz.api

object RunQuickPojo {
  def main(args: Array[String]): Unit = {
    val pojo = new QuickPojo()
    pojo.featureId = "1"
    println(pojo.featureId)

  }

  class QuickPojo extends Enumeration{
     var _featureId: String = ""

    def featureId: String = _featureId

    def featureId_=(value: String): Unit = {
      _featureId = value
    }
  }
}
