package com.drobot.streaming.entity

/**
 * Case class representing information about hotel booking.
 *
 * @param hotel_id          hotel id.
 * @param duration          stay duration in days.
 * @param srch_children_cnt number of children.
 * @author Uladzislau Drobat
 */
case class HotelStayInfo(hotel_id: Long,
                         duration: Int,
                         srch_children_cnt: Int)
