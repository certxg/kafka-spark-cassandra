package com.ajit.example.kafka.spark.cassandra

import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder

class ReadMessagesFromKafka {

  def main(args: Array[String]) {

    val sc = new SparkConf(true)
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[12]")
      .setAppName("Streaming Kafka App")

    val ssc = new StreamingContext(sc, Seconds(2))

    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "zookeeper.connection.timeout.ms" -> "10000",
      "group.id" -> "myGroup")
      
      val topics = Map(
        "ajit" -> 1
  )

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)
    val messages =  stream.map(_._2)
      
      val words = messages.flatMap(_.split(" "))
      
      words.print();

  }

}