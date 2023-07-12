package veera.tcstest.spark.fxstockanalyzer

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.language.implicitConversions

object FxOrderEngine extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      logger.error("Usage: FxOrderEngine ")
      System.exit(1)
    }

    logger.info("Starting FxOrderEngine")
    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()
    //logger.info("spark.conf=" + spark.conf.getAll.toString())

    val orderRawDF = loadOrderDF(spark, args(0))
    val partitionedorderDF = orderRawDF.repartition(2)

    val buyOrdersDF = orderTypeFilter(partitionedorderDF,"BUY")
    val sellOrdersDF = orderTypeFilter(partitionedorderDF,"SELL")
    val matchedOrderDF = matchedOrderFilter(buyOrdersDF,sellOrdersDF)

    matchedOrderDF.foreach(row => {
      logger.info("matchedOrderDF: " + row.getString(0) + " Count: " + row.getLong(1))
    })
    matchedOrderDF.show()

    logger.info(matchedOrderDF.collect().mkString("->"))

    logger.info("Finished FxOrderEngine")

    spark.stop()
  }



  def loadOrderDF(spark: SparkSession, dataFile: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataFile)
  }

  def orderTypeFilter(orderDF: DataFrame,filterVal:String): DataFrame = {
    orderDF.filter(orderDF("orderType") === filterVal)
  }

  def matchedOrderFilter (buyOrders: DataFrame,sellOrders:DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy().orderBy("orderTime")

    val matchedOrderDF = buyOrders.join(
      sellOrders,
      buyOrders("quantity") === sellOrders("quantity") &&
        buyOrders("price") === sellOrders("price"),
      "inner"
    ).select(
      buyOrders("orderId").alias("buyOrderId"),
      sellOrders("orderId").alias("sellOrderId"),
      buyOrders("price"),
      buyOrders("quantity"),
      buyOrders("orderTime")
    ).withColumn("rowNum", row_number().over(windowSpec))

    matchedOrderDF.filter(matchedOrderDF("rowNum") === 1).drop("rowNum")

  }


  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }

}
