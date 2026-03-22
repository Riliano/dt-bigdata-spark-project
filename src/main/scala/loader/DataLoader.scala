package loader

/* Apache Spark */
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

/* Log4j */
import org.apache.log4j.{Level, Logger}

import java.io.File
import java.io.FileNotFoundException

object DataLoader {

  val defaultFilePath = "./dataset/cardsTransactions.csv"
  val logger = Logger.getLogger("CardDWH")

  def loadTransactionsCSV(
    spark: SparkSession, 
    inputFilePath: String = defaultFilePath
  ): DataFrame = {

    val input = new File(inputFilePath)

    if (!input.exists()) {
      logger.error("Input CSV cannot be found!")
      throw new FileNotFoundException(
        "Could not find input file at: " + inputFilePath)
    }

    /**
     * Schema was derived when observing assignment description and the
     * first few lines of the input file
     */
    val inputFileSchema = StructType(Array(
      StructField("pid", IntegerType, true),
      StructField("pname", StringType, true),
      StructField("age", IntegerType, true),
      StructField("gender", StringType, true),
      StructField("cardno", StringType, true),
      StructField("card_brand", StringType, true),
      StructField("card_type", StringType, true),
      StructField("tdate", TimestampType, true),
      StructField("amount", DoubleType, true),
      StructField("ttc", IntegerType, true),
      StructField("trans_type", StringType, true),
      StructField("mcc", IntegerType, true),
      StructField("merchant_city", StringType, true)
    ))

    logger.info("Begin reading input file")
    val transactionsDf = spark.read
      .format("csv")
      .schema(inputFileSchema)
      .option("header", "true")
      .option("delimiter", "|")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load(input.getCanonicalPath())
      .withColumn("tid", monotonically_increasing_id() + 1) //Add transaction id
      .select("tid", "*") //rearrange

    logger.info("Input file imported")
    return transactionsDf
  }
}
