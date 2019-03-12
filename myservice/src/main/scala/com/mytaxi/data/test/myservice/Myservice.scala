package com.mytaxi.data.test.myservice

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.log4j.{Level, Logger}
import com.mytaxi.data.test.kafkasink.{KafkaSink}
import com.mytaxi.data.test.taxiservice.{
        PaymentService,
        DriversEarning
        }

class Myservice extends Serializable {

  def empty(id_driver: Int): DriversEarning = {
    DriversEarning(id_driver,0.0)
  }

  def updateSessionEvents(
      id: Int,
      payments: Iterator[PaymentService],
      state: GroupState[DriversEarning]): DriversEarning = {
    val oldState: DriversEarning = if (state.exists) state.get else empty(id)
    val updatedState = payments.foldLeft(oldState) { (baseState,event) =>
        baseState.tour_value = baseState.tour_value + event.tour_value
      baseState
    }
    state.update(updatedState)
    updatedState
  }
}

object Myservice extends App with LazyLogging {

  //implicit val paymentServiceEncoder: Encoder[PaymentService] = Encoders.kryo[PaymentService]
  //implicit val driversEarningEncoder: Encoder[DriversEarning] = Encoders.kryo[DriversEarning]

  logger.info("starting myservice...")

  val kafkaWriter: KafkaSink = new KafkaSink()

  val spark = SparkSession
    .builder()
    .appName("myservice")
    .config("spark.testing.memory", "2147480000")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  val mySchema = StructType(
    Array(
      StructField("id", IntegerType),
      StructField("event_date", StringType),
      StructField("tour_value", DoubleType),
      StructField("id_driver", IntegerType),
      StructField("id_passenger", IntegerType)
    ))

  val paymentStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "payment")
    .load()
    .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
    .as[(String, Timestamp)]

  val paymentStreamSession: Dataset[PaymentService] = paymentStream
    .select(from_json($"value", mySchema).as("data"), $"timestamp")
    .select($"data.id",
            $"data.event_date",
            $"data.tour_value",
            $"data.id_driver",
            $"data.id_passenger")
    .map { event =>
      PaymentService(event.getAs[Int](0),
                     event.getAs[String](1),
                     event.getAs[Double](2),
                     event.getAs[Int](3),
                     event.getAs[Int](4))
    }

  // Life-time Aggregate of a driver. This is cumulative sum and requires stateful aggregation on events

  val cumulative_agg: Dataset[DriversEarning] = paymentStreamSession
    .groupByKey(event => event.id_driver)
    .mapGroupsWithState(GroupStateTimeout.NoTimeout())(
      new Myservice().updateSessionEvents
    )

  cumulative_agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("5 seconds"))    
    .start()

  // Driver's recent performance with 1 minute interval

  val oneMinuteAggregates = paymentStreamSession
      .withColumn("event_date",from_unixtime($"event_date"/1000).cast("timestamp"))
      .withWatermark("event_date","60 seconds") // watermark to handle late data. bclearut keeping it same as window.
      .groupBy(window($"event_date","60 seconds"),$"id_driver") // tumbling window of 1 minute
      .agg(sum($"tour_value").alias("payment_per_minute"),count($"id").alias("tours_per_minute"))
      .select($"id_driver",$"payment_per_minute",$"tours_per_minute")
      .filter($"payment_per_minute" > 200)

  oneMinuteAggregates.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime("5 seconds"))    
    .start()

  // write to Kafka Sink topic
  oneMinuteAggregates.select(to_json(struct($"id_driver", $"payment_per_minute", $"tours_per_minute")) as "value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers","kafka:9092")
    .option("checkpointLocation", "/tmp/kafkacheckpoint/")
    .option("topic","drivers")
    .outputMode("complete")
    .start()

  // while spark structured streaming offers kafka sink which is very straightforward , we can also create Kafka Producer by extending ForeachWriter to sink the events in.
  oneMinuteAggregates.select(to_json(struct($"id_driver", $"payment_per_minute", $"tours_per_minute")) as "value")
    .writeStream
    .foreach(kafkaWriter.kafkaWriter)
    .outputMode("update")
    .start()

  spark.streams.awaitAnyTermination()

}
