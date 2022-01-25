package com.cqz.test

object ScalaObjectTest {
  def main(args: Array[String]): Unit = {
    val unit = Account.newUniqueNum()
    val unit2 = Account.newUniqueNum()
    println(unit)
    println(unit2)
    val a = ApplyObject(1)
    println(a)
  }
}
