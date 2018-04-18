package com.github.bjkonglu.operator

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

object SocketWatermark {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SocketWatermark <hostname> " +
        "<port> <window duration in seconds> [<slide duration in seconds>]")
      System.exit(-1)
    }
    val host = args(0)
    val port = args(1).toInt
    val windowSize = args(2).toInt
    val slideSize = if (args.length == 3) windowSize else args(3).toInt

    if (slideSize > windowSize) {
      System.err.println("<slide duration> must be less than or equal to <window duration>")
    }
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SocketWatermark")
      .getOrCreate()

    import sparkSession.implicits._

    val line = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()

    val words = line.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2)))
      .toDF("word", "timestamp")

    val windowedCounts = words
      /**
        * watermark可以处理延时数据
        * watermark = [max even time seen by the engine] - [late threshold]
        * 如果事件时间大于watermark，则事件可以被更新
        * */
      .withWatermark("timestamp", "2 seconds")
      .groupBy(window($"timestamp", windowDuration, slideDuration), $"word")
      .count().orderBy("word")

    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
