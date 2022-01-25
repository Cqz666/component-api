package com.cqz.test

object Account {
  private var lastNum = 0

  def newUniqueNum(): Int ={
    lastNum+=1
    lastNum
  }
}
