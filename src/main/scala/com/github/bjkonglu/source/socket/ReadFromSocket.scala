package com.github.bjkonglu.source.socket

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * Structured Streaming DEMO
  *
  */
object ReadFromSocket {

  val logger = Logger(classOf[App])

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("ReadFromSocket")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val lines = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(100))
      .start()

    query.awaitTermination()
  }
}
