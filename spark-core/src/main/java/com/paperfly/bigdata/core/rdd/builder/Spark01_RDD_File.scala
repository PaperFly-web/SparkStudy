package com.paperfly.bigdata.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//读取内存中的数据，创建RDD
object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {
    //todo 创建配置环境
    //local[*]：表示使用本地环境，*表示使用多少个核数，*就是有多少使用多少
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    val sc = new SparkContext(conf)

    //TODO 创建RDD
    //从文件中获取数据,path:可以是相对路径，也可以是绝对路径。相对路径相对于项目根路径
    //path也可以指向路径，会对路径下的所有文件进行统计
    val rdd: RDD[String] = sc.textFile("data/1.txt",2)
    rdd.map((s:String)=>{
      "["+s+"]"
    })
    rdd.collect().foreach(println)


    //todo 关闭RDD
    sc.stop()
  }

  /*def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 从文件中创建RDD，将文件中的数据作为处理的数据源
    // path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
    //sc.textFile("D:\\mineworkspace\\idea\\classes\\atguigu-classes\\datas\\1.txt")
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")
    // path路径可以是文件的具体路径，也可以目录名称
    //val rdd = sc.textFile("datas")
    // path路径还可以使用通配符 *
    //val rdd = sc.textFile("datas/1*.txt")
    // path还可以是分布式存储系统路径：HDFS
    val rdd = sc.textFile("hdfs://linux1:8020/test.txt")
    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }*/
}
