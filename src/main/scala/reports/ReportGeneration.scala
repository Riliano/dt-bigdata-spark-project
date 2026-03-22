package reports

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
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
}
