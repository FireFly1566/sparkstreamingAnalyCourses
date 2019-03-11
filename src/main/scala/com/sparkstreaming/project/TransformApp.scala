package com.sparkstreaming.project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
* 黑名单过滤
* */

object TransformApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 构建黑名单
    val blacks = List("zs", "ls")
    val blachRDD = ssc.sparkContext.parallelize(blacks).map((_, true))


    val lines = ssc.socketTextStream("localhost", 6789)

    val clicklog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blachRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)
    })

    clicklog.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
