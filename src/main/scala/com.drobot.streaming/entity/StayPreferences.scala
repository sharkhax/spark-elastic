package com.drobot.streaming.entity

/**
 * Case class representing hotel stay preferences based on stay durations and children presence.
 *
 * @param hotelId              hotel id.
 * @param batchTimestamp       timestamp of current batch.
 * @param withChildren         counts of children presence.
 * @param erroneousData        counts of erroneous data.
 * @param shortStay            counts of short stays.
 * @param standartStay         counts of standart stays.
 * @param standartExtendedStay count of standart extended stays.
 * @param longStay             count of long stays.
 * @param mostPopularStay      most popular stay type.
 * @author Uladzislau Drobat
 */
case class StayPreferences(hotelId: Long,
                           var batchTimestamp: String,
                           var withChildren: Long,
                           var erroneousData: Long,
                           var shortStay: Long,
                           var standartStay: Long,
                           var standartExtendedStay: Long,
                           var longStay: Long,
                           var mostPopularStay: String) {

  /**
   * Constructor of empty object where hotel id is only specified.
   *
   * @param hotelId hotel id.
   */
  def this(hotelId: Long) = this(hotelId, null, 0, 0, 0, 0, 0, 0, null)

  /**
   * Adds preference based on stay duration.
   *
   * @param hotelStayInfo HotelStayInfo object which contains stay duration.
   */
  def addDurationPreference(hotelStayInfo: HotelStayInfo): Unit = {
    hotelStayInfo.duration match {
      case 1 => shortStay += 1
      case x if 2 to 6 contains x => standartStay += 1
      case x if 7 to 13 contains x => standartExtendedStay += 1
      case x if 14 to 30 contains x => longStay += 1
      case _ => erroneousData += 1
    }
  }

  /**
   * Adds preference based on stay duration and children presence.
   *
   * @param hotelStayInfo HotelStayInfo object which contains stay duration and number of children.
   */
  def addExtendedPreference(hotelStayInfo: HotelStayInfo): Unit = {
    addDurationPreference(hotelStayInfo)
    if (hotelStayInfo.srch_children_cnt > 0) {
      withChildren += 1
    }
  }

  /**
   * Updates most popular stay type.
   */
  def updateMostPopularStay(): Unit = {
    val preferences: Array[Long] = Array(shortStay, standartStay, standartExtendedStay, longStay)
    var mostPopular = 0
    for (i <- 1 until preferences.length) {
      if (preferences.apply(i) > preferences.apply(0)) {
        mostPopular = i
      }
    }
    var result: String = null
    mostPopular match {
      case 0 => result = "short_stay"
      case 1 => result = "standart_stay"
      case 2 => result = "standart_extended_stay"
      case 3 => result = "long_stay"
    }
    mostPopularStay = result
  }
}
