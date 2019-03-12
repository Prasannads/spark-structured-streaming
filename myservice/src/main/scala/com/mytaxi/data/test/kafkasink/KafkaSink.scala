package com.mytaxi.data.test.kafkasink

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer,ProducerRecord}
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

class KafkaSink extends Serializable {

    val topic = "drivers"
    val brokers = "kafka:9092"
    val key = "drivers"

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", brokers)
    kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    var producer: KafkaProducer[String,String] = _

    val kafkaWriter = new ForeachWriter[Row] {

    def open(partitionId: Long, version: Long): Boolean = {
        producer = new KafkaProducer(kafkaProperties)
        true
    }

    def process(value: Row): Unit = {
        producer.send(new ProducerRecord(topic, key, value.getString(0)))
    }

    def close(errorOrNull: Throwable): Unit = {
        producer.close()
    }
    }
}