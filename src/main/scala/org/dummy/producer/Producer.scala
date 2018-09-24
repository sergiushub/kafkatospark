package org.dummy.producer

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.dummy.producer.TwitterStream.OnTweetPosted
import twitter4j._

object Producer {

  private val conf = ConfigFactory.load()
  private val KafkaTopic = "topic1"

  val kafkaProducer = {
    val config = new Properties()
    config.put("bootstrap.servers",conf.getString("kafka.brokers"))
    new KafkaProducer[String, String](config, new StringSerializer, new StringSerializer)
  }

  val filterUsOnly = new FilterQuery().track("es")


  private def getInfoTweet(s: Status): String = {
    s.getPlace().getCountry()
  }

  private def sendToKafka(s:String) {
    val msg = new ProducerRecord[String, String](KafkaTopic, s)
    kafkaProducer.send(msg)
  }

  def main (args: Array[String]) {
    val twitterStream = TwitterStream.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(getInfoTweet(s))))
    twitterStream.filter(filterUsOnly)
  }

}

