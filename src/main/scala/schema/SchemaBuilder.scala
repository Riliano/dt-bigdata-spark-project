package schema
/* Apache Spark */
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

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
    /*
    val cardCols = Seq("cardno", "card_brand", "card_type")
    val dimCards = mdf.select(cardCols.map(col): _*).distinct
     .withColumn("cid", row_number().over(Window.orderBy("cardno")))

    dimCards.printSchema()
    dimCards.show(10, false)
    println("Num cards: " + dimCards.count())
    */

    /* Drop everything to create the fact table */
    /* We use .tail to skip the first element, which should always be the key */
    val colsToDrop = customerCols.tail ++ transTypeCols.tail ++ merchantCols.tail
    val factDf = df.drop(colsToDrop: _*)
    factDf.printSchema()
    factDf.show(10, false)

    return Map(
      "DimCustomers" -> dimCustomers,
      "DimTransTypes" -> dimTransTypes,
      "DimMerchants" -> dimMerchants,

      "Fact" -> factDf
    )
  }
}
