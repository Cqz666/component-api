package com.cqz.test

class ApplyObject(val id:Int,val name:String) {

}

object ApplyObject{
  def apply(id:Int): Unit ={
  new ApplyObject(id,"n")
  }
}
