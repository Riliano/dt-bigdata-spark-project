package schema

import org.apache.spark.sql.DataFrame

sealed trait DimensionKey
case object FactKey extends DimensionKey
case object DimCustomersKey extends DimensionKey
case object DimDateKey extends DimensionKey
case object DimTransTypesKey extends DimensionKey
case object DimCardsKey extends DimensionKey
case object DimMerchantsKey extends DimensionKey

case class TransactionDWH(
  Fact: DataFrame,
  DimCustomers: DataFrame,
  DimDate: DataFrame,
  DimTransTypes: DataFrame,
  DimCards: DataFrame,
  DimMerchants: DataFrame
) {
  // Iterable view 
  def dims: Seq[(String, DataFrame)] = Seq(
    "DimCustomers" -> DimCustomers,
    "DimTransTypes" -> DimTransTypes,
    "DimMerchants" -> DimMerchants,
    "DimCards" -> DimCards,
    "DimDate" -> DimDate,
    "Fact" -> Fact
  )
  def dimsMap: Map[String, DataFrame] = dims.toMap
}

object TransactionDWH {
  sealed trait DimensionKey
  case object FactKey extends DimensionKey
  case object DimCustomersKey extends DimensionKey
  case object DimDateKey extends DimensionKey
  case object DimTransTypesKey extends DimensionKey
  case object DimCardsKey extends DimensionKey
  case object DimMerchantsKey extends DimensionKey

  val FactCols: Seq[String] = 
    Seq("tid", "pid", "cid", "ttc", "mcc", "date_key", "tdate", "amount")
  val DimCustomersCols: Seq[String] = Seq("pid", "pname", "age", "gender")
  val DimDateCols: Seq[String] = 
    Seq("date_key", "date", "year", "quarter", "month", "month_name", "day")
  val DimTransTypesCols: Seq[String] = Seq("ttc", "trans_type")
  val DimCardsCols: Seq[String] = 
    Seq("cid", "cardno", "card_brand", "card_type")
  val DimMerchantsCols: Seq[String] = Seq("mcc", "merchant_city")

  val columnMap: Map[DimensionKey, Seq[String]] = Map(
    FactKey -> FactCols,
    DimCustomersKey -> DimCustomersCols,
    DimDateKey -> DimDateCols,
    DimTransTypesKey -> DimTransTypesCols,
    DimCardsKey -> DimCardsCols,
    DimMerchantsKey -> DimMerchantsCols
  )

  def cols(key: DimensionKey): Seq[String] = columnMap(key)
}
