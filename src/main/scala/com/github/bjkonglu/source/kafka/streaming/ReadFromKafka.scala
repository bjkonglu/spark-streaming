package com.github.bjkonglu.source.kafka.streaming

import org.apache.spark.sql.SparkSession

object ReadFromKafka {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ReadFromKafka")
      .getOrCreate()

    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.144.110:9888,192.168.144.111:9888,192.168.144.112:9888")
      .option("subscribe", "streaming-2018-04-18")
      .option("startingOffsets", "earliest")
      .load()

    df.selectExpr("CAST(value as STRING)", "topic")
      .createOrReplaceTempView("tableName")

    //利用sparkSession.sql接口查询DF
    val newDs = sparkSession.sql("select * from tableName")
      .writeStream
      .format("console")
      .outputMode("update")
      .start()

    newDs.awaitTermination()

  }
}
