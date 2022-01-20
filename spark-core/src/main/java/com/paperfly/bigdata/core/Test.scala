package com.paperfly.bigdata.core

import math._

object Test {
 /* def main(args: Array[String]): Unit = {
    val test = new Myclass2 with Mytrait2 {
      @Override
      override def forfun(): Unit = {
        println("666")
      }
    }
    val test2 = new Myclass1

    test forfun()
    test2 doSomething ("Test")
  }*/

}

trait Mytrait1 {
  def doSomething(str: String): Unit = {
    println(s"$str is doing")
  }
}

trait Mytrait2 {
  def forfun(): Unit
}

class Myclass3 extends Mytrait1 with Mytrait2 {
  override def forfun(): Unit = ???
}

/*
class Myclass4 with Mytrait2 {
  override def forfun (): Unit = ???
}

class Myclass5 extends Mytrait2 with Myclass3 {
  override def forfun(): Unit = ???
}

class Myclass6 extends Myclass3 with Mytrait2 with Mytrait1 {
  override def forfun(): Unit = ???
}
*/
