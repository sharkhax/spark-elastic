package com.drobot.streaming

import com.drobot.streaming.`type`.PreferenceType
import com.drobot.streaming.entity.{Expedia, Hotel, HotelStayInfo, StayPreferences}
import com.drobot.streaming.state.PreferenceState
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/**
 * Object represents the main login of spark-streaming application.
 *
 * @author Uladzislau Drobat
 */
object PreferencesApplication {

  private final val spark = SparkSession
    .builder()
    .appName("Test na22me")
    .master(Constant.MASTER_URL)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()

  import spark.implicits._

  /**
   * Getter method of spark session.
   *
   * @return SparkSession object of the application.
   */
  def session: SparkSession = {
    spark
  }

  /**
   * Runs the application.
   */
  def runApplication(): Unit = {
    val expediaStreamingDF = expediaStream(Constant.EXPEDIA_YEAR_VIA_STREAMING)
    val hotelsDF = hotelsBatch
    val initialPreferenceStateMap = PreferenceState.getInstance().value
    val hotelStayInfos = hotelStayInfo(expediaStreamingDF, hotelsDF)
    val extendedPreferences = calculateExtendedPreferences(hotelStayInfos, Option.apply(initialPreferenceStateMap))
    writeResultToElastic(extendedPreferences)
  }

  /**
   * Reads enriched hotels as a batch.
   *
   * @return DataFrame of enriched hotels.
   */
  def hotelsBatch: DataFrame = {
    val hotelSchema = Hotel.schema
    val hotelsDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", Constant.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", Constant.ENRICHED_HOTELS_TOPIC)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .select(from_json(col("value").cast("string"), hotelSchema).alias("data"))
      .select("data.*")
    hotelsDF
  }

  /**
   * Reads expedia data as a batch by specified year.
   *
   * @param year by which expedia data will be read.
   * @return DataFrame of expedia.
   */
  def expediaBatch(year: Int): DataFrame = {
    val expediaDF = spark.read
      .format("avro")
      .schema(Expedia.schema)
      .load(Constant.EXPEDIA_DATA_PATH + $"year=$year/*")
    expediaDF
  }

  /**
   * Reads expedia data as a stream by specified year.
   *
   * @param year by which expedia data will be read.
   * @return streaming DataFrame of expedia.
   */
  def expediaStream(year: Int): DataFrame = {
    val expediaDF = spark.readStream
      .format("avro")
      .schema(Expedia.schema)
      .load(Constant.EXPEDIA_DATA_PATH + $"year=$year/*")
    expediaDF
  }

  /**
   * Returns hotels' stay info from expedia and hotels DataFrames after joining them.
   * Also validates rows by having positive average Celsius temperature.
   *
   * @param expediaDF DataFrame of expedia.
   * @param hotelsDF  DataFrame of hotels.
   * @return Dataset of HotelStayInfo objects.
   */
  def hotelStayInfo(expediaDF: DataFrame, hotelsDF: DataFrame): Dataset[HotelStayInfo] = {
    val joinCondition = (expediaDF.col("hotel_id") === hotelsDF.col("id"))
      .and(expediaDF.col("srch_ci") === hotelsDF.col("wthr_date"))
    val stayDuration = expediaDF.join(hotelsDF, joinCondition)
      .select("hotel_id", "srch_children_cnt", "srch_ci", "srch_co", "avg_tmpr_c")
      .filter("avg_tmpr_c > 0")
      .withColumn("duration", expr("dateDiff(srch_co, srch_ci)"))
      .select("hotel_id", "duration", "srch_children_cnt")
      .as[HotelStayInfo]
    stayDuration
  }

  /**
   * Calculates hotel preferences based on its stay durations.
   *
   * @param hotelStayInfoDS DataSet of HotelStayInfo objects.
   * @param optionalStates  Option object of map of initial stay preferences states (hotel id as a key).
   *                        If it's empty, empty StayPreferences object will be used.
   * @return DataSet of StayPreferences objects.
   */
  def calculateDurationPreferences(hotelStayInfoDS: Dataset[HotelStayInfo],
                                   optionalStates: Option[Map[Long, StayPreferences]]): Dataset[StayPreferences] = {
    calculatePreferences(hotelStayInfoDS, optionalStates, PreferenceType.DURATION_ONLY)
  }

  /**
   * Calculates hotel preferences based on its stay durations and children presence.
   *
   * @param hotelStayInfoDS DataSet of HotelStayInfo objects.
   * @param optionalStates  Option object of map of initial stay preferences states (hotel id as a key).
   *                        If it's empty, empty StayPreferences object will be used.
   * @return DataSet of StayPreferences objects.
   */
  def calculateExtendedPreferences(hotelStayInfoDS: Dataset[HotelStayInfo],
                                   optionalStates: Option[Map[Long, StayPreferences]]): Dataset[StayPreferences] = {
    calculatePreferences(hotelStayInfoDS, optionalStates, PreferenceType.EXTENDED)
  }

  private def calculatePreferences(hotelStayInfoDS: Dataset[HotelStayInfo],
                                   optionalStates: Option[Map[Long, StayPreferences]],
                                   preferenceType: PreferenceType): Dataset[StayPreferences] = {
    val mappingFunction = preferencesMappingFunction(_, _, _, optionalStates, preferenceType)
    val preferences = hotelStayInfoDS
      .groupByKey(_.hotel_id)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(func = mappingFunction)
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(0), 1625310204))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(1), 1625310214))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(2), 1625310224))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(3), 1625310234))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(4), 1625310244))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(5), 1625310254))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(6), 1625310264))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(7), 1625310274))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(8), 1625310284))
      .withColumn("batchTimestamp", when(col("batchTimestamp").isNull and col("hotel_id").%(10).===(9), 1625310294))
      .as[StayPreferences]
    preferences
  }

  private def preferencesMappingFunction(hotelId: Long,
                                         hotelStayInfos: Iterator[HotelStayInfo],
                                         state: GroupState[StayPreferences],
                                         optionalStates: Option[Map[Long, StayPreferences]],
                                         preferenceType: PreferenceType): Iterator[StayPreferences] = {
    lazy val initialState = initState(hotelId, optionalStates)
    val currentState = state.getOption.getOrElse(initialState)
    addPreference(currentState, hotelStayInfos, preferenceType)
    currentState.updateMostPopularStay()
    state.update(currentState)
    Iterator(currentState)
  }

  private def initState(hotelId: Long,
                        optionalStates: Option[Map[Long, StayPreferences]]): StayPreferences = {
    var currentState: StayPreferences = null
    lazy val defaultState = new StayPreferences(hotelId)
    if (optionalStates.isDefined) {
      currentState = optionalStates.get.getOrElse(hotelId, defaultState)
    } else {
      currentState = defaultState
    }
    currentState
  }

  private def addPreference(currentState: StayPreferences,
                            hotelStayInfos: Iterator[HotelStayInfo],
                            preferenceType: PreferenceType): Unit = {
    preferenceType match {
      case PreferenceType.EXTENDED =>
        while (hotelStayInfos.hasNext) {
          val hotelStayInfo = hotelStayInfos.next()
          currentState.addExtendedPreference(hotelStayInfo)
        }
      case PreferenceType.DURATION_ONLY =>
        while (hotelStayInfos.hasNext) {
          val hotelStayInfo = hotelStayInfos.next()
          currentState.addDurationPreference(hotelStayInfo)
        }
      case _ => throw new EnumConstantNotPresentException(preferenceType.getClass, preferenceType.toString)
    }
  }

  private def writeResultAsAvro[T](dataset: Dataset[T]): Unit = {
    dataset
      .writeStream
      .format("avro")
      .option("checkpointLocation", Constant.CHECKPOINTING_DIRECTORY)
      .outputMode(OutputMode.Append())
      .start(Constant.PREFERENCES_PATH)
      .awaitTermination()
  }

  private def writeResultToElastic[T](dataset: Dataset[T]): Unit = {
    dataset
      .writeStream
      .format("es")
      .outputMode(OutputMode.Append())
      .option("es.port", "9200")
      .option("es.nodes", "localhost")
      .option("es.nodes.wan.only", "true")
      .option("checkpointLocation", "/spark-elastic_logs3")
      .start("preferences")
      .awaitTermination()
  }

  private def writeResultToConsole[T](dataset: Dataset[T]): Unit = {
    dataset
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .option("truncate", "false")
      .option("numRows", "100")
      .option("checkpointLocation", Constant.CHECKPOINTING_DIRECTORY)
      .start()
      .awaitTermination()
  }

  private def writeResultAsJson[T](dataset: Dataset[T]): Unit = {
    dataset
      .writeStream
      .format("json")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", "/spark-elastic_logs")
      .start("/dataset/preferences_json")
      .awaitTermination()
  }
}
