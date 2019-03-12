package com.mytaxi.data.test.myservice

import org.scalatest._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.functions._
import com.mytaxi.data.test.taxiservice.{
        PaymentService,
        DriversEarning,
        }
import com.mytaxi.data.test.myservice
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import com.mytaxi.data.test.util.{InMemoryStoreWriter,InMemoryKeyedStore}


class MainSpec extends FlatSpec with Matchers {

  val spark = SparkSession
    .builder()
    .appName("mapGroupsWithStateTest")
    .config("spark.testing.memory", "2147480000")
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  "There is at least one test for this project" should "be true" in {
    0 should equal(0)
  }

  "cummulative agg with update mode" should "work for mapGroupsWithState" in {
      val key = "mapGroupsWithState-update-mode" 
      val InMemoryWriter: InMemoryStoreWriter = new InMemoryStoreWriter()
      val inputStream = new MemoryStream[(PaymentService)](1, spark.sqlContext)
      val payments: Dataset[PaymentService] = inputStream.toDS()
      val mappedValues = payments.groupByKey(event => event.id_driver)
                                    .mapGroupsWithState(GroupStateTimeout.NoTimeout())(new Myservice().updateSessionEvents _)
      inputStream.addData(PaymentService(9973538,"1552220980052",62.77,0,16),
                          PaymentService(922996,"1552220979045",21.61,4,72), 
                          PaymentService(922997,"1552220979046",121.27,4,73),
                          PaymentService(2339444,"1552220978043",57.425,3,77))
                      
      val testQuery = mappedValues.select(concat($"id_driver".cast("string"),lit(","),$"tour_value".cast("string")) as "value")
        .writeStream
        .outputMode("update")
        .foreach(InMemoryWriter.InMemoryWriter).start()

      new Thread(new Runnable() {
      override def run(): Unit = {
        while (!testQuery.isActive) {}
        Thread.sleep(1000)
        inputStream.addData(PaymentService(2339444,"1552220953005",3.123,13,66), PaymentService(2339444,"1552220952004",9.88,14,74), PaymentService(2339444,"1552220951000",6.425,10,22))
      }
      }).start()

      testQuery.awaitTermination(10000)

      val outputValues = InMemoryKeyedStore.getValues(key)
      outputValues should have size 6
      outputValues should contain allOf("3,57.425", "4,142.88", "0,62.77", "13,3.123", "10,6.425", "14,9.88")
  }
}
