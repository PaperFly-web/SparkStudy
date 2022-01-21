package com.paperfly.bigdata.streaming

import com.paperfly.bigdata.utils.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object SparkStreaming11_req1 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux1:9092,linux2:9092,linux3:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "bigdata",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("bigdata"), kafkaPara)
    )

    //TODO 1.将数据装换为  广告类
    val adClickDS: DStream[AdClickData] = kafkaDataDS.map(v => {
      val value: Array[String] = v.value().split(" ")
      AdClickData(value(0), value(1), value(2), value(3), value(4))
    })

    //用户不在黑名单中的广告点击统计
    val ds: DStream[((String, String, String), Int)] = adClickDS.transform(
      rdd => {
        // TODO 2.通过JDBC周期性获取黑名单数据
        val blackList = ListBuffer[String]()
        val cnn: Connection = JDBCUtil.getConnection
        val preparedStatement: PreparedStatement = cnn.prepareStatement("select userid from black_list")
        val resultSet: ResultSet = preparedStatement.executeQuery()
        while (resultSet.next()) {
          blackList.append(resultSet.getString(1))
        }

        resultSet.close()
        preparedStatement.close()
        cnn.close()

        // TODO 3.判断点击用户是否在黑名单中
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )
        // TODO 4.如果用户不在黑名单中，那么进行统计数量（每个采集周期）
        val wordToOneRDD: RDD[((String, String, String), Int)] = filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new Date(data.ts.toLong))
            val user = data.user
            val ad: String = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
        wordToOneRDD
      }
    )

    ds.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, user, ad), count) => {
            println(s"${day} ${user} ${ad} ${count}")

            if (count > 30) {
              //TODO 5.如果用户当前点击量就已经超过阈值，就直接拉入黑名单
              val conn = JDBCUtil.getConnection
              val pstat = conn.prepareStatement(
                """
                  |insert into black_list (userid) values (?)
                  |on DUPLICATE KEY
                  |UPDATE userid = ?
                                """.stripMargin)
              pstat.setString(1, user)
              pstat.setString(2, user)
              pstat.executeUpdate()
              pstat.close()
              conn.close()
            } else {
              // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新。
              val conn = JDBCUtil.getConnection
              val pstat = conn.prepareStatement(
                """
                  | select
                  |     *
                  | from user_ad_count
                  | where dt = ? and userid = ? and adid = ?
                                """.stripMargin)

              pstat.setString(1, day)
              pstat.setString(2, user)
              pstat.setString(3, ad)
              val rs = pstat.executeQuery()
              // 查询统计表数据
              if (rs.next()) {
                // 如果存在数据，那么更新
                val pstat1 = conn.prepareStatement(
                  """
                    | update user_ad_count
                    | set count = count + ?
                    | where dt = ? and userid = ? and adid = ?
                                    """.stripMargin)
                pstat1.setInt(1, count)
                pstat1.setString(2, day)
                pstat1.setString(3, user)
                pstat1.setString(4, ad)
                pstat1.executeUpdate()
                pstat1.close()
                // TODO 判断更新后的点击数据是否超过阈值，如果超过，那么将用户拉入到黑名单。
                val pstat2 = conn.prepareStatement(
                  """
                    |select
                    |    *
                    |from user_ad_count
                    |where dt = ? and userid = ? and adid = ? and count >= 30
                                    """.stripMargin)
                pstat2.setString(1, day)
                pstat2.setString(2, user)
                pstat2.setString(3, ad)
                val rs2 = pstat2.executeQuery()
                if (rs2.next()) {
                  val pstat3 = conn.prepareStatement(
                    """
                      |insert into black_list (userid) values (?)
                      |on DUPLICATE KEY
                      |UPDATE userid = ?
                                        """.stripMargin)
                  pstat3.setString(1, user)
                  pstat3.setString(2, user)
                  pstat3.executeUpdate()
                  pstat3.close()
                }

                rs2.close()
                pstat2.close()
              } else {
                // 如果不存在数据，那么新增
                val pstat1 = conn.prepareStatement(
                  """
                    | insert into user_ad_count ( dt, userid, adid, count ) values ( ?, ?, ?, ? )
                                    """.stripMargin)

                pstat1.setString(1, day)
                pstat1.setString(2, user)
                pstat1.setString(3, ad)
                pstat1.setInt(4, count)
                pstat1.executeUpdate()
                pstat1.close()
              }

              rs.close()
              pstat.close()
              conn.close()
            }
          }
        }


      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  // 广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
