package com.TwitterStreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object ScalaStreamingKafka {

  def main(args: Array[String]) {

    // Set logs to display ERROR only
    StreamingExamples.setStreamingLogLevels()

    // create streaming context and session
    val sparkConf = new SparkConf().setAppName("ScalaStreamingKafka").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val ss = SparkSession
      .builder
      .appName("KafkaReceiver")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .getOrCreate()

    // define Kafka params
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // create stream
    val topics = Array("twitter")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // extract raw tweet
    val raw_tweets = stream.map(x => x.value())

    // parse to JSON
    val json_tweets = raw_tweets.map(x => scala.util.parsing.json.JSON.parseFull(x)
      .get.asInstanceOf[Map[String, Any]])

    // filter english tweets
    val parsed_tweets = json_tweets.filter(x => x.get("lang").mkString == "en")

    // extract id & text
    val filtered_tweets = parsed_tweets.map(x => Row(x.get("id").mkString, x.get("text").mkString))

    // create schema
    val schema = StructType(
      Array(
        StructField("id", StringType, true),
        StructField("text", StringType, true)
      )
    )

    // convert to df & append to Hive
    filtered_tweets.foreachRDD { rdd =>
      val df = ss.createDataFrame(rdd, schema)
      df.show()
      df.write.mode(SaveMode.Append).saveAsTable("default.twitter_scala_tbl")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}