package com.github.bjkonglu.source.kafka.batch

import org.apache.spark.sql.SparkSession

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ReadFromKafka")
      .getOrCreate()

    import sparkSession.implicits._

    val df = sparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.144.110:9888,192.168.144.111:9888,192.168.144.112:9888")
      .option("subscribe", "streaming-2018-04-18")
      .option("startingOffsets", """{"streaming-2018-04-18" : {"2" : 0,"1" : 0,"0" : 0} }""")
      .option("endingOffsets", """{"streaming-2018-04-18" : {"2" : 3,"1" : 3,"0" : 3} }""")
      .load()

//    val columnNames: Array[String] = Array("name", "age")

    df.selectExpr("CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
      .as[(String, String, Int, Long, Long)]
      .map(line => line._1)
      .map(value => {
        val columns = value.split(" ")
        val column1 = columns(0)
        val column2 = columns(1)
        Tuple2(column1, column2)
      })
      .toDF("name", "age")
      .write
      .format("console")
      .save()

    //    df.selectExpr("CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
    //      .as[(String, String, Int, Long, Long)]
    //      .write
    //      .format("console")
    //      .save()
  }
}
