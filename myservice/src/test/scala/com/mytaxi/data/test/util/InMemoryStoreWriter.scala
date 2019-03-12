package com.mytaxi.data.test.util

import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

class InMemoryStoreWriter extends Serializable { 
  val key = "mapGroupsWithState-update-mode"  

  val InMemoryWriter = new ForeachWriter[Row] {
    def open(partitionId: Long, version: Long): Boolean = true

    def process(value: Row): Unit = {
        InMemoryKeyedStore.addValue(key, value.getString(0))
    }

    def close(errorOrNull: Throwable): Unit = {}
    }
}