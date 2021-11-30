package com.paperfly.bigdata.core

import math._

object Test {
  var s: String = null
  var s2: String = _

  def main(args: Array[String]): Unit = {
    /*var ll: List[Int] = List(5, 1, 2, 3, 4)
    var l2: List[Int] = List(6, 7, 8, 9)
    ll = l2 ::: ll
    ll = l2.::(ll)
    ll = l2 ++ ll
    ll = 6 +: ll
    ll = ll :+ 6

    ll.foreach(x => {
      println(x)
    })*/
    val myMap: Map[String, String] = Map("key1" -> "value")
    val value1: Option[String] = myMap.get("key1")
    val value2: Option[String] = myMap.get("key2")



    println(value1.getOrElse(0)) // Some("value1")
    println(value2.getOrElse(0)) // None
    println(None.getOrElse(0))

  }

  def sum(args: Int*): Int = {
    var total: Int = 0
    for (arg <- args) {
      total = total + arg
    }
    total
  }
}
