package com.cqz.test

object ScalaClassTest {
  def main(args: Array[String]): Unit = {
    val mycount = new Count
    mycount.increase()
    mycount.increase()
    println(mycount.current)

    val myperson = new Person
    myperson.age = 30
    myperson.age = 20
    println(myperson.age)

    val bean = new Bean
    bean.setName("bean")
    bean.name="a"
    val name = bean.getName
    println(name)

    val x = new Person2("x",1)
    val age = x.age
    val name1 = x.name

    println("age "+age+" name "+ name1)


  }
}
