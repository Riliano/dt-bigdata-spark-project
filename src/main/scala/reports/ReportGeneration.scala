package reports

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import schema.TransactionDWH

object TransactionReports {

  /** 
   * Report 1: Total transaction value per city 
   * Output: ("City", "TotalTransactionValue")
   */
  def totalByCity(dwh: TransactionDWH): DataFrame = {
    dwh.Fact
      .join(dwh.DimMerchants, Seq("mcc"), "left")
      .groupBy("merchant_city")
      .agg(round(sum("amount"), 2).alias("TotalTransactionValue"))
      .orderBy(desc("TotalTransactionValue"))
  }

  /**
   * Report 2: Total transaction value per year and gender
   * Output: ("Year", "Gender", "TotalTransactionValue")
   */
  def totalByYearGender(dwh: TransactionDWH): DataFrame = {
    dwh.Fact
      .join(dwh.DimCustomers, Seq("pid"), "left")
      .join(dwh.DimDate, Seq("date_key"), "left")
      .groupBy("year", "gender")
      .agg(round(sum("amount"), 2).alias("TotalTransactionValue"))
      .orderBy(desc("year"))
  }

  /**
   * Report 3: Number and total transaction value per card brand and type
   * Output: ("CardBrand", "CardType", "NumTransactions", "TotalTransactionValue")
   */
  def totalByCardBrandType(dwh: TransactionDWH): DataFrame = {
    dwh.Fact
      .join(dwh.DimCards, Seq("cid"), "left")
      .groupBy("card_brand", "card_type")
      .agg(
        count("*").alias("NumTransactions"),
        round(sum("amount"), 2).alias("TotalTransactionValue")
      )
      .orderBy("card_brand")
  }

  /**
   * Report 4: Quarterly transaction value per transaction type for 2019
   * Output: ("Quarter", "TransactionType", "TotalTransactionValue")
   */
  def quarterlyByTransType2019(dwh: TransactionDWH): DataFrame = {
    dwh.Fact
      .join(dwh.DimTransTypes, Seq("ttc"), "left")
      .join(dwh.DimDate, Seq("date_key"), "left")
      .filter(col("year") === 2019)
      .groupBy("quarter", "trans_type")
      .agg(round(sum("amount"), 2).alias("TotalTransactionValue"))
      .orderBy("quarter")
  }

  /**
   * Report 5: Number of transactions per year, card brand, and gender (data cube)
   * Output: ("Year", "CardBrand", "Gender", "NumTransactions")
   */
  def cubeTransactions(dwh: TransactionDWH): DataFrame = {
    // Alias all DataFrames to avoid ambiguous columns
    val fact = dwh.Fact.as("f")
    val cards = dwh.DimCards.as("c")
    val customers = dwh.DimCustomers.as("cu")
    val dates = dwh.DimDate.as("d")
    fact
      .join(cards, col("f.cid") === col("c.cid"), "left")
      .join(customers, col("f.pid") === col("cu.pid"), "left")
      .join(dates, col("f.date_key") === col("d.date_key"), "left")
      .cube(col("d.year"), col("c.card_brand"), col("cu.gender"))
      .agg(count("*").alias("NumTransactions"))
      .orderBy(col("d.year"), col("c.card_brand"), col("cu.gender"))
  }

  /** Custom reports **/

  /**
   * Report 6: Monthly trend
   * Output: ("Year", "Month", "MonthName", "NumTransactions", "TotalTransactionValue", AverageTransactionValue")
   */
  def seasonalTrends(dwh: TransactionDWH): DataFrame = {
    dwh.Fact
      .join(dwh.DimDate, dwh.Fact("date_key") === dwh.DimDate("date_key"))
      .groupBy("year", "month", "month_name")
      .agg(
        count("*").as("NumTransactions"),
        sum(dwh.Fact("amount")).as("TotalTransactionValue"),
        avg(dwh.Fact("amount")).as("AvgTransactionValue")
      )
      .orderBy(col("Year").asc, col("Month").asc)
  }

  /**
   * Report 7: Customer value concentration by spend decile
   * Output: ("Decile", "Numbef of Customers", "TotalTransactions", "TotalTransactionValue", "ShareOfTotalValue")
   */

  def customerValueDecile(dwh: TransactionDWH): DataFrame = {
    val customerTotals = dwh.Fact
      .groupBy(col("pid"))
      .agg(
        count("*").as("NumTransactions"),
        sum(col("amount")).as("TotalTransactionValue")
      )

      val winSpec = Window.orderBy(col("TotalTransactionValue").desc)
      val ranked = customerTotals.withColumn("Decile", ntile(10).over(winSpec))

      val result = ranked
        .groupBy(col("Decile"))
        .agg(
          count("*").as("NumCustomers"),
          sum(col("NumTransactions")).as("TotalTransactions"),
          sum(col("TotalTransactionValue")).as("TotalTransactionValue")
        )
        .orderBy(col("Decile").asc)

      return result
  }
}
