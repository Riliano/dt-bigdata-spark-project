/* Apache Spark */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

/* Log4j */
import org.apache.log4j.{Level, Logger}

import java.io.File
import java.io.FileNotFoundException



object cardDWH {

  val inputFilePath = "./dataset/cardsTransactions.csv"

  def main(args: Array[String]) {

    /* Initalization and setup */
    println("Hello Scala! ")
    val conf = new SparkConf().setAppName("cardDWH")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    
    /* Reduce log clutter */
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val logger = Logger.getLogger("CardDWH")

    val input = new File(inputFilePath)

    if (!input.exists()) {
      logger.error("Input CSV cannot be found!")
      throw new FileNotFoundException("Could not find input file at: " + inputFilePath)
    }

    /**
     * Example first two lines from CSV
pid|pname|age|gender|cardno|card_brand|card_type|tdate|amount|ttc|trans_type|mcc|merchant_city
1|Hazel Robinson|53|F|4001482973848631|Visa|Debit|2015-01-03 13:59:00|98.69|3|Chip Transaction|6844|Little Neck
1|Hazel Robinson|53|F|4001482973848631|Visa|Debit|2015-01-04 06:52:00|108.20|3|Chip Transaction|2459|Fresh Meadows

*/
    /**
     * Schema of our input file
     */
    val inputFileSchema = StructType(Array(
      StructField("pid", IntegerType, false),
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

    logger.info("Input file imported")
    
    transactionsDf.printSchema()
    transactionsDf.show(10, false)

  }
}
