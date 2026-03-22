/* Apache Spark */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/* Log4j */
import org.apache.log4j.{Level, Logger}

/* Custom */
import loader.DataLoader
import schema.{SchemaBuilder, TransactionDWH}
import reports.TransactionReports

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


    val transactionsDf = DataLoader.loadTransactionsCSV(spark)
    
    /* Just for debug purposes */
    transactionsDf.printSchema()
    transactionsDf.show(10, false)

    val schema = SchemaBuilder.buildTransactionStartSchema(transactionsDf)

    schema.dimsMap.foreach { case (name, df) => 
      DataLoader.exportToCsv(df, name, "output/schema")
     // println("Here is: " + name)
     // df.printSchema()
     // df.show(10, false)
    }

    logger.info("Beginning report process")
    def runReport(
      name: String, 
      f: TransactionDWH => DataFrame, 
      dwh: TransactionDWH
    ): (String, DataFrame) = {
      logger.info(s"Starting report: $name")
      val df = f(dwh)
      logger.info(s"Finished report: $name")
      return (name, df)
    }

    val reports: Seq[(String, DataFrame)] = Seq(
      runReport("TotalByCity", TransactionReports.totalByCity, schema),
      runReport("TotalByYearGender",
        TransactionReports.totalByYearGender,
        schema),
      runReport("ByCardBrandType",
        TransactionReports.totalByCardBrandType,
        schema),
      runReport("Quarterly2019", 
        TransactionReports.quarterlyByTransType2019, 
        schema),
      runReport("CubeTransactions", TransactionReports.cubeTransactions, schema)
    )

    reports.foreach { case (name, df) =>
      DataLoader.exportToCsv(df, name, "output/reports")
    }

  }
}
