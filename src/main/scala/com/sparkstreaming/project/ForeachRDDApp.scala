package com.sparkstreaming.project

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming完成有状态的统计，并将结果写入MySQL数据库
  *
  */

object ForeachRDDApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.socketTextStream("localhost", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    //state.print()

    // 会产生序列化错误
    //    result.foreachRDD(rdd => {
    //      val connection = createConnection() // driver
    //      rdd.foreach { record =>
    //        val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
    //        connection.createStatement().execute(sql)
    //      }
    //    })

    result.print()

    result.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecord => {

        val connection = createConnection()
        partitionOfRecord.foreach(pair => {
          val sql = "insert into wordcount(word, wordcount) values('" + pair._1 + "'," + pair._2 + ")"
          connection.createStatement().execute(sql)
        })

        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  // 获取 MySQL 连接
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_streaming?useSSL=false", "root", "root")
  }

}
