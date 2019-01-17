import scala.io.Source
import org.apache.spark.{SparkContext,SparkConf}

object OrderRevenue{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Order Revenue")
    val sc = new SparkContext(conf)

    // broadcast variables - broadcast products and look up for product name
    val base_path = "/Users/nj/data/retail_db"
    val orders_data_path = "/orders/part-00000"
    val ordersItems_data_path = "/order_items/part-00000"
    val product_data_path = "/products/part-00000"

    val orders = sc.textFile(base_path + orders_data_path)
    val ordersCompletedCount = sc.accumulator(0, "Orders Completed Count")
    val ordersNonCompletedCount = sc. accumulator(0, "Number of orders that are not completed")
    val ordersFiltered = orders.
      filter(order => {
        val isCompletedOrder = order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED"
        if(isCompletedOrder) ordersCompletedCount += 1
        else ordersNonCompletedCount += 1
        isCompletedOrder
      })

    val ordersMap = ordersFiltered.map(order => {
      (order.split(",")(0).toInt, order.split(",")(1))
    })

    val orderItems = sc.textFile(base_path + ordersItems_data_path)
    val orderItemsMap = orderItems.map(orderItem => {
      (orderItem.split(",")(1).toInt, (orderItem.split(",")(2).toInt, orderItem.split(",")(4).toFloat))
    })

    val ordersJoin = ordersMap.join(orderItemsMap)
    val ordersJoinMap = ordersJoin.map(rec => ((rec._2._1, rec._2._2._1), rec._2._2._2))
    val dailyRevenuePerProductId = ordersJoinMap.reduceByKey((total, revenue) => total + revenue)

    val products = Source.fromFile(base_path + product_data_path).getLines.toList
    val productsMap = products.map(product => (product.split(",")(0).toInt, product.split(",")(2))).toMap
    val bv = sc.broadcast(productsMap)
    val dailyRevenuePerProductName = dailyRevenuePerProductId.map(rec => {
      ((rec._1._1, -rec._2), rec._1._1 + "," + rec._2 + "," + bv.value.get(rec._1._2).get)
    })
    val dailyRevenuePerProductNameSorted = dailyRevenuePerProductName.
      sortByKey().
      map(rec => rec._2)

    dailyRevenuePerProductNameSorted.saveAsTextFile(base_path+"/output")
  }
}