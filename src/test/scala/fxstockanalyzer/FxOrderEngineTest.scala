package fxstockanalyzer

import veera.tcstest.spark.fxstockanalyzer.FxOrderEngine.{loadOrderDF,orderTypeFilter,matchedOrderFilter}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable

class FxOrderEngineTest extends FunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("FxOrderEngineTest")
      .master("local[3]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("FxOrderEngineTest Data File Loading") {
    val sampleDF = loadOrderDF(spark,"data/order.csv")
    val rCount = sampleDF.count()
    assert(rCount==8, " record count should be 8")
  }

  test("Count by Buy"){
    val sampleDF = loadOrderDF(spark,"data/order.csv")
    val buycountDF = orderTypeFilter(sampleDF,"BUY")
    val sellcountDF = orderTypeFilter(sampleDF,"SELL")


    assert(buycountDF.count() == 4, ":- Count for buycountDF should be 4")
    assert(sellcountDF.count() == 4, ":- Count for buycountDF should be 4")
  }

}
