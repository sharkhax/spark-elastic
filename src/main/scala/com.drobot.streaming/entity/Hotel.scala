package com.drobot.streaming.entity

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
 * Case class represents enriched hotel.
 *
 * @author Uladzislau Drobat
 */
case class Hotel(id: Long, name: String, country: String, city: String, address: String, longitude: Double,
                 latitude: Double, geohash: String, wthr_date: String, avg_tmpr_f: Double, avg_tmpr_c: Double)

object Hotel {
  /**
   * Returns spark schema of hotel.
   *
   * @return spark schema as StructType.
   */
  def schema: StructType = {
    val hotelSchema = ScalaReflection.schemaFor[Hotel].dataType.asInstanceOf[StructType]
    hotelSchema
  }
}
