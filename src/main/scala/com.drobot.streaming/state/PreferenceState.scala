package com.drobot.streaming.state

import com.drobot.streaming.entity.StayPreferences
import com.drobot.streaming.{Constant, PreferencesApplication}
import org.apache.spark.broadcast.Broadcast

/**
 * Object that manages broadcast variable (initial preference states) of spark application.
 *
 * @author Uladzislau Drobat
 */
object PreferenceState {

  private var preferenceState: Broadcast[Map[Long, StayPreferences]] = null
  private val spark = PreferencesApplication.session
  private val sc = spark.sparkContext

  /**
   * Gives a singleton instance of broadcast variable.
   *
   * @return broadcast variable containing map of StayPreferences objects with their hotel ids as keys.
   */
  def getInstance(): Broadcast[Map[Long, StayPreferences]] = {
    if (preferenceState == null) {
      synchronized(
        if (preferenceState == null) {
          preferenceState = sc.broadcast(calculatePreferences)
        }
      )
    }
    preferenceState
  }

  private def calculatePreferences: Map[Long, StayPreferences] = {
    val hotelsDF = PreferencesApplication.hotelsBatch
    val expediaDF = PreferencesApplication.expediaBatch(Constant.EXPEDIA_YEAR_VIA_BATCHING)
    val stayDurationDF = PreferencesApplication.hotelStayInfo(expediaDF, hotelsDF)
    val preferencesState = PreferencesApplication.calculateDurationPreferences(stayDurationDF, Option.empty)
    var map = Map[Long, StayPreferences]()
    preferencesState.foreach(preference => map = map + (preference.hotelId -> preference))
    map
  }
}
