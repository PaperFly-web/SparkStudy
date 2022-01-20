package com.paperfly.bigdata.core

object FunctionTest {
 
  def main(args: Array[String]): Unit = {
//    test(plus)

    test(){
      println("传入的逻辑1")
      println("传入的逻辑2")
    }
  }
 
  def test(a:String="a")(fun: => Unit): Unit = {
    println("this is test run","参数A="+a)
    //执行传入来的逻辑
    fun
  }
 
  def plus() = {
    println("this is plus run")
  }
 
}