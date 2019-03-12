package com.mytaxi.data.test.paymentservice

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object ConfluentProducer extends LazyLogging {

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "payment_producer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val key: String = "payment"

  def send(topic: String, payload: String): Unit = {
    val record = new ProducerRecord[String, String](topic, key, payload)
    producer.send(record)
  }
}
