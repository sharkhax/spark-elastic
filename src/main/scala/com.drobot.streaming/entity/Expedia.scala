package com.drobot.streaming.entity

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
 * Case class representing expedia.
 *
 * @author Uladzislau Drobat
 */
case class Expedia(id: Long, date_time: String, site_name: String, posa_continent: Int, user_location_country: Int,
                   user_location_region: Int, user_location_city: Int, orig_destination_distance: Double, user_id: Int,
                   is_mbile: Int, is_package: Int, channel: Int, srch_ci: String, srch_co: String, srch_adults_int: Int,
                   srch_children_cnt: Int, srch_rm_cnt: Int, srch_destination_id: Int, srch_destination_type_id: Int,
                   hotel_id: Long)

object Expedia {
  /**
   * Returns spark schema of expedia.
   *
   * @return spark schema as StructType.
   */
  def schema: StructType = {
    ScalaReflection.schemaFor[Expedia].dataType.asInstanceOf[StructType]
  }
}
