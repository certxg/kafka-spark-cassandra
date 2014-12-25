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

object QueryTweets {

  def main(args: Array[String]) {
    System.setProperty("spark.cassandra.query.retry.count", "1") //dont retry

    val conf = new SparkConf(true)
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cleaner.ttl", "3600")
      .setMaster("local[12]")
      .setAppName("Streaming Kafka App")

    val sc = new SparkContext(conf)

   val rdd = sc.cassandraTable("mykeyspace", "tweets")
   rdd.toArray.foreach(println)
    
 

  }

}