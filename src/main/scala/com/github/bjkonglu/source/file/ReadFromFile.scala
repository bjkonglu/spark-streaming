package com.github.bjkonglu.source.file

import org.apache.spark.sql.SparkSession

object ReadFromFile {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ReadFromFile")
      .getOrCreate()

    val ds = sparkSession.readStream
      .text("/Users/lukong/netease-projects/spark-streaming")

    val query = ds.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
