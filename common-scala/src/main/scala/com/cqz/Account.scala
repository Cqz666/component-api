package com.cqz

object Account {
  private var lastNum = 0

  def newUniqueNum(): Int ={
    lastNum+=1
    lastNum
  }
}
