/* Apache Spark */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

/* Log4j */
import org.apache.log4j.{Level, Logger}

/* Custom */
import loader.DataLoader
import schema.SchemaBuilder



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

    schema.foreach { case (name, df) => 
      println("Here is: " + name)
      df.printSchema()
      df.show(10, false)
    }

  }
}
