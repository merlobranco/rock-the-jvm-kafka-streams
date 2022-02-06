package content

object KafkaStreams {

  object Domain {
    type UserId = String
    type Profile = String
    type Product = String
    type OrderId = String
    type Status = String

    case class Order(orderId: OrderId, user: UserId, products: List[Product], amount: Double)

    case class Discount(profile: Profile, amount: Double) // in percentage points

    case class Payment(orderId: OrderId, status: Status)
  }

  object Topics {
    final val OrdersByUserTopic = "orders-by-user"
    final val DiscountProfilesByUserTopic = "discount-profiles-by-user"
    final val DiscountsTopic = "discounts"
    final val OrdersTopic = "orders"
    final val PaymentsTopic = "payments"
    final val PaidOrdersTopic = "paid-orders"
  }

  def main(args: Array[String]): Unit = {
    List(
      "orders-by-user",
      "discount-profiles-by-user",
      "discounts",
      "orders",
      "payments",
      "paid-orders"
    ).foreach {
      topic =>
        println(s"kafka-topics --bootstrap-server localhost:9092 --topic ${topic} --create")
    }
  }
}
