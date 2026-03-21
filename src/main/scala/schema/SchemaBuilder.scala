package schema
/* Apache Spark */
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/* Log4j */
import org.apache.log4j.{Level, Logger}

object SchemaBuilder {

  val logger = Logger.getLogger("CardDWH")

  def buildTransactionStartSchema(df: DataFrame): Map[String, DataFrame] = {

    /**
     * First Dimension - customer general infocmation
     * The first dimension would be for the specific customers. It also follows
     * naturally, as the master key is already defined in the Dataset 
     */

    val customerCols = Seq("pid", "pname", "age", "gender")
    val dimCustomers = df.select(customerCols.map(col): _*).distinct()

    logger.info("Customer Dimension extracted")
    logger.info("Num customers: " + dimCustomers.count())

    /**
     * Second Dimension - Transaction types
     * Comes naturally as well for already having an ID in the dataset
     */

    val transTypeCols = Seq("ttc", "trans_type")
    val dimTransTypes = df.select(transTypeCols.map(col): _*).distinct()

    logger.info("Transaction Type Dimension extracted")
    logger.info("Types of Transaction: " + dimTransTypes.count())

    /**
     * Third Dimension - Merchant
     * Again, comes naturally as it already has it's code
     */

    val merchantCols = Seq("mcc", "merchant_city")
    val dimMerchants = df.select(merchantCols.map(col): _*).distinct()

    logger.info("Merchant Dimension extracted")
    logger.info("Number of merchants: " + dimMerchants.count())

    /**
     * Fourth dimension - card
     */
    val cardCols = Seq("cardno", "card_brand", "card_type")
    /* Create the window with a partition, as to avoid perf issues */
    val dimCards = df.select(cardCols.map(col): _*).distinct
 //     .repartition(1)
      .withColumn("cid", monotonically_increasing_id() + 1)
      .select(("cid" +: cardCols).map(col): _*) /* For reordering */

    logger.info("Cards Dimension extracted")
    logger.info("Number of cards: " + dimCards.count())

    /* Drop everything to create the fact table */
    /* We use .tail to skip the first element, which should always be the key */
    val colsToDrop = customerCols.tail ++ 
                     transTypeCols.tail ++
                     merchantCols.tail
    val factNatDf = df.drop(colsToDrop: _*)

    val factWithCards = factNatDf
      .join(dimCards, cardCols, "left")
      .drop(cardCols: _*)

    return Map(
      "DimCustomers" -> dimCustomers,
      "DimTransTypes" -> dimTransTypes,
      "DimMerchants" -> dimMerchants,
      "DimCards" -> dimCards,

      "Fact" ->  /* Reorder */
        factWithCards.select("pid", "cid", "ttc", "mcc", "tdate", "amount")
    )
  }
}
