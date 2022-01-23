package com.cqz

class Count {
  private var value =0
  def increase(): Unit ={
    value+=1
  }
  def current = value
}
