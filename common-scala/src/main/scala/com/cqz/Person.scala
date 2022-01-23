package com.cqz

class Person {
  private var privateAge = 0

  def age = privateAge

  def age_=(newValue:Int): Unit ={
    if (newValue>privateAge) privateAge = newValue
  }

}
