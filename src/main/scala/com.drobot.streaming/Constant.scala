package com.drobot.streaming

/**
 * Object represents constants values that are used in the application.
 */
object Constant {

  /**
   * Year of expedia data to read as a stream.
   */
  final val EXPEDIA_YEAR_VIA_STREAMING:Int = 2017

  /**
   * Year of expedia data to read as a batch.
   */
  final val EXPEDIA_YEAR_VIA_BATCHING:Int = 2016

  /**
   * Path of expedia data.
   */
  final val EXPEDIA_DATA_PATH = "/dataset/valid_expedia2/"

  /**
   * Kafka bootstrap servers config.
   */
  final val KAFKA_BOOTSTRAP_SERVERS = "host.docker.internal:9094"

  /**
   * Name of kafka topic with enriched hotels.
   */
  final val ENRICHED_HOTELS_TOPIC = "enriched-hotels-topic"

  /**
   * Name of the application.
   */
  final val APP_NAME = "Spark-streaming"

  /**
   * Master URL spark config.
   */
  final val MASTER_URL = "local[3]"

  /**
   * Path to write preferences.
   */
  final val PREFERENCES_PATH = "/dataset/hotels_preferences"

  /**
   * Path of checkpointing directory.
   */
  final val CHECKPOINTING_DIRECTORY = "/dataset/checkpoint/spark_streaming"
}