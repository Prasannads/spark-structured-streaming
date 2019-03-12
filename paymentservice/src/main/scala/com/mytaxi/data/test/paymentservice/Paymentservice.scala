package com.mytaxi.data.test.paymentservice

import com.typesafe.scalalogging.LazyLogging

object Paymentservice extends App with LazyLogging {
  logger.info("starting paymentservice...")
  while (true) {
    val topic = "payment"
    val r = scala.util.Random
    val id = r.nextInt(10000000)
    val tour_value = r.nextDouble() * 100
    val id_driver = r.nextInt(10)
    val id_passenger = r.nextInt(100)
    val event_date = System.currentTimeMillis
    val payload =
      s"""
         |{ "id": $id,
         |   "event_date": $event_date,
         |   "tour_value": $tour_value,
         |   "id_driver": $id_driver,
         |   "id_passenger": $id_passenger
         |}
      """.stripMargin
    logger.debug(payload)
    ConfluentProducer.send(topic, payload)
    Thread.sleep(1000)
  }
}
