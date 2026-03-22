package schema
/* Apache Spark */
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/* Log4j */
import org.apache.log4j.{Level, Logger}

object SchemaBuilder {
  /**
  * IMPORTANT
  * We get the column definitions as well as the col helper from here
  */
  import TransactionDWH._

  val logger = Logger.getLogger("CardDWH")

  /* helper */
  /* This is used in order to automatically extracted
   * all dimensoin columns from a dataframe */
  def selectExistingByKey(df: DataFrame, key:DimensionKey): DataFrame = {
    val existing = cols(key).filter(df.columns.contains)
    df.select(existing.map(col): _*)
  }

//  def buildTransactionStartSchema(df: DataFrame): Map[String, DataFrame] = {
  def buildTransactionStartSchema(df: DataFrame): TransactionDWH = {

    /**
     * First Dimension - customer general infocmation
     * The first dimension would be for the specific customers. It also follows
     * naturally, as the master key is already defined in the Dataset 
     */
    val dimCustomers = selectExistingByKey(df, DimCustomersKey).distinct()
    logger.info("Customer Dimension extracted")
    logger.info("Num customers: " + dimCustomers.count())

    /**
     * Second Dimension - Transaction types
     * Comes naturally as well for already having an ID in the dataset
     */
    val dimTransTypes = selectExistingByKey(df, DimTransTypesKey).distinct()
    logger.info("Transaction Type Dimension extracted")
    logger.info("Types of Transaction: " + dimTransTypes.count())

    /**
     * Third Dimension - Merchant
     * Again, comes naturally as it already has it's code
     */

    val dimMerchants = selectExistingByKey(df, DimMerchantsKey).distinct()
    logger.info("Merchant Dimension extracted")
    logger.info("Number of merchants: " + dimMerchants.count())

    /**
     * Fourth dimension - card
     */
    val dimCards = selectExistingByKey(df, DimCardsKey).distinct()
 //     .repartition(1)
      .withColumn("cid", monotonically_increasing_id().cast("int") + 1)
      .select(cols(DimCardsKey).map(col): _*) /* For reordering */

    logger.info("Cards Dimension extracted")
    logger.info("Number of cards: " + dimCards.count())

    /* Drop everything to create the fact table */
    /* We use .tail to skip the first element, which should always be the key */
    val colsToDrop = cols(DimCustomersKey).tail ++ 
                     cols(DimTransTypesKey).tail ++
                     cols(DimMerchantsKey).tail
    val factNatDf = df.drop(colsToDrop: _*)

    val existingCardCols = cols(DimCardsKey).filterNot(_ == "cid")
    val factWithCards = factNatDf
      .join(dimCards, existingCardCols, "left")
      .drop(existingCardCols: _*)

    /**
     * Date Dimension
     * Extract only the date portion of the timestamp for eaier query later only
     * However keep the transacton time into the fact table, for better balance
     * between the two tables
     */
    val dimDate = df
      .select("tdate")
      .withColumn("date", to_date(col("tdate")))
      .select("date")
      .distinct()
      .withColumn("date_key",  date_format(col("date"), "yyyyMMdd").cast("int"))
      .withColumn("year", year(col("date")))
      .withColumn("quarter", quarter(col("date")))
      .withColumn("month", month(col("date")))
      .withColumn("month_name", date_format(col("date"), "MMMM"))
      .withColumn("day", dayofmonth(col("date")))
      .select(cols(DimDateKey).map(col): _*) /* rearrange */
    logger.info("Date Dimension extracted")
    logger.info("Number of dates: " + dimDate.count())

    /* Instead of a join, we'll collapse tdate into our date_key */
    val factWithDate = factWithCards
      .withColumn("date_key", date_format(col("tdate"), "yyyyMMdd").cast("int"))
//    .drop("tdate")

    return TransactionDWH(
      DimCustomers = dimCustomers,
      DimTransTypes = dimTransTypes,
      DimMerchants = dimMerchants,
      DimCards = dimCards,
      DimDate = dimDate,
      Fact = factWithDate.select(cols(FactKey).map(col): _*) /* Reorder */
      /*
    return Map(
      "DimCustomers" -> dimCustomers,
      "DimTransTypes" -> dimTransTypes,
      "DimMerchants" -> dimMerchants,
      "DimCards" -> dimCards,
      "DimDate" -> dimDate,
      "Fact" -> 
      */
    )
  }
}
