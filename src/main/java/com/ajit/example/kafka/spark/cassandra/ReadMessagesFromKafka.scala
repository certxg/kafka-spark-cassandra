package com.ajit.example.kafka.spark.cassandra

import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.writer.WriteConf

object ReadMessagesFromKafka {

  def main(args: Array[String]) {
    
   val sc = new SparkConf(true)
    .set("spark.cassandra.connection.host",  "127.0.0.1")
    .set("spark.cleaner.ttl", "3600")
    .setMaster("local[12]")
    .setAppName("Streaming Kafka App")
    
    CassandraConnector(sc).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS mykeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS mykeyspace.tweets (key VARCHAR PRIMARY KEY, value INT)")
    session.execute(s"TRUNCATE mykeyspace.tweets")
  }   

    val ssc = new StreamingContext(sc, Seconds(2))

    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "zookeeper.connection.timeout.ms" -> "10000",
      "group.id" -> "myGroup")

    val topics = Map("ajit-topic" -> 1)

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)
    val messages = stream.map(_._2)

    val words = messages.flatMap(_.split(" "))

    words.print();
    words.map(x => (x, 1)).saveToCassandra("mykeyspace", "tweets", SomeColumns("key", "value"), WriteConf.fromSparkConf(sc))  

    ssc.start();

  }

}