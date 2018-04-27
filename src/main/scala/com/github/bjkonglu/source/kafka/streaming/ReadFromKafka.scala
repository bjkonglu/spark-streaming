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
      .option("kafka.bootstrap.servers", "bjstream2.dg.163.org:9092,bjstream3.dg.163.org:9092,bjstream4.dg.163.org:9092")
      .option("subscribe", "datastream.datacenter_exp_etl")
//      .option("startingOffsets", "earliest")
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
