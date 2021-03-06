package wsc.bigdata.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by weishungchung on 7/2/17.
 */
object AnalyzeInstaCartData {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("Analyzing Instacart Data")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    //val sqlContext = new HiveContext(sc)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    //load departments data
    val departmentDataInputPath = args(0)
    val departments = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load(departmentDataInputPath)
    departments.printSchema()

    //load aisles data
    val aisleDataInputPath = args(1)
    val aisles = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load(aisleDataInputPath)
    aisles.printSchema()

    //load products data
    val productDataInputPath = args(2)
    val products = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load(productDataInputPath)
    products.printSchema()

    //load orders data
    val orderDataInputPath = args(3)
    val orders = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load(orderDataInputPath)
    orders.printSchema()

    //load train data
    val trainDataInputPath = args(4)

    //group by hour of day
    val groupByHourOfDay = orders.groupBy("order_hour_of_day").count().sort(desc("count"))
    groupByHourOfDay.show()

    orders.registerTempTable("orders")

    val groupByHourOfDayUseSQL = sqlContext.sql("select order_hour_of_day, count(1) as count from orders group by order_hour_of_day order by count DESC")
    groupByHourOfDayUseSQL.show()

    //group by day of week
    val groupByDow = sqlContext.sql("select order_dow, count(1) as count from orders group by order_dow order by count")
    groupByDow.show()

    //group by days since prior order
    val groupByDaysPriorOrder = sqlContext.sql("select days_since_prior_order,count(1) as count from orders group by days_since_prior_order order by count DESC")
    groupByDaysPriorOrder.show()

    val groupByUserIdPriorOrderSQL = sqlContext.sql("select user_id, days_since_prior_order, count(1) as count from orders group by user_id, days_since_prior_order order by user_id, count DESC")

    //group by both user and days since prior order
    val groupByUserIdPriorOrder = orders.groupBy("user_id","days_since_prior_order").count().sort(asc("user_id"),desc("count"))
    groupByUserIdPriorOrder.show()

    //figure out the most typical days since prior order for every user
    val windowSpec = Window.partitionBy("user_id").orderBy(desc("count"))
    val userReorderPeriod = groupByUserIdPriorOrder.withColumn("row",rowNumber().over(windowSpec)).filter("row =1").drop("row")
    userReorderPeriod.show()

    val trainData = sqlContext.read.format("com.databricks.spark.csv").option("header","true").option("inferSchema","true").load(trainDataInputPath)
    trainData.printSchema()
    
    val orderProductUser = trainData.join(orders, trainData("order_id") === orders("order_id"))
    //val orderProductJoin = trainData.join(products, trainData("product_id") === products("product_id"))
    val groupByProduct = trainData.groupBy("product_id").count().sort(desc("count"))
    groupByProduct.join(products.select("product_id", "product_name"), groupByProduct("product_id") === products("product_id")).sort(desc("count")).show(false)

    val groupByUserProduct = orderProductUser.groupBy("user_id","product_id").count().sort(asc("user_id"),desc("count"))
    val windowSpecByUserProduct = Window.partitionBy("user_id").orderBy(desc("count"))
    val userPopularProduct = groupByUserProduct.withColumn("row",rowNumber().over(windowSpecByUserProduct)).filter("row =1").drop("row")
    userPopularProduct.filter("count > 1")

  }
}
