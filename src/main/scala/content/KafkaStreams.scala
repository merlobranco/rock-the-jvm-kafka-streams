package content

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties


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

  // Source: emits elements
  // Flows (or pipes): transforms elements along the way (e.g. map)
  // Sinks: "ingests" elements

  import Domain._
  import Topics._

  implicit def serde[A >: Null : Decoder : Encoder]: Serde[A] = {
    val serializer = (a: A) => a.asJson.noSpaces.getBytes
    val deserializer = (aAsBytes: Array[Byte]) => {
      val aAsString = new String(aAsBytes)
      val aOrError = decode[A](aAsString)
      aOrError match {
        case Right(a) => Option(a)
        case Left(error) =>
          println(s"There was an error converting the message $aOrError, $error")
          Option.empty
      }
    }
    Serdes.fromFn[A](serializer, deserializer)
  }

  def main(args: Array[String]): Unit = {

    // Topology. Describes how the data flows through the stream
    val builder = new StreamsBuilder()

    // KStream (Linear kafka stream)
    val usersOrdersStream: KStream[UserId, Order] = builder.stream[UserId, Order](OrdersByUserTopic)

    // KTable: like KStream, except the elements that "flows" through the stream are a little more static in the sense that they are
    // kept inside the topic using a time to live policy and they are corresponding to a topic that we call compacted.
    // Compacted means that elements that flow through that topic are maintained inside the broker

    val userProfileTable: KTable[UserId, Profile] = builder.table[UserId, Profile](DiscountProfilesByUserTopic)

    // GlobalKTable
    // Keeping in mind that each topic has partitions.
    // While the KTable is distributed between the nodes in the kafka cluster
    // the GlobalKTable is not distributed but rather copied to all the nodes in the cluster
    // Useful for performance purposes since the GlobalKTable will be faster than the KTable, since the GlobalKTable is already on memory
    // The GlobalKTable is going to be useful as long as we store a few values inside
    val discountProfilesGTable: GlobalKTable[Profile, Discount] = builder.globalTable[Profile, Discount](DiscountsTopic)

    // KStream transformation: filter, map, mapValues, flatMap, flatMapValues
    val expensiveOrders = usersOrdersStream.filter { (userId, order) =>
      order.amount > 1000
    }

    val listsOfProducts = usersOrdersStream.mapValues { order =>
      order.products
    }

    // Individual stream of products identified by the same user ied
    val productsStream = usersOrdersStream.flatMapValues(_.products)

    // Join
    // The Join operation will return those records for which the same identifier was encountered,
    // usersOrdersStream contains orders identified by userId
    // userProfileTable contains profiles also identified by userId
    // We have 2 entities identified by the same userId
    // So for the same userId a pair of (order, profile) will be returned
    val ordersWithUserProfiles = usersOrdersStream.join(userProfileTable) { (order, profile) =>
      (order, profile)
    }

    val discountedOrderStream = ordersWithUserProfiles.join(discountProfilesGTable)(
      { case (userId, (order, profile)) => profile }, // First we should provide the key of the join from the first stream
      { case ((order, profile), discount) => order.copy(amount = order.amount - discount.amount) } // Values of the matched records: From the pair of the returned values we will build our desired result
    )

    // Pick another identifier
    val ordersStream = discountedOrderStream.selectKey((userId, order) => order.orderId)

    // We could join also stream with another stream, on the previous examples we were joining streams with tables
    // we were joining something dynamic with something static.
    // This join between 2 streams could be done under certain conditions:
    //   1) We need a join window in between which kafka can do the matching between these 2 streams, otherwise if a record arrives too late, it will be discarded,
    //      otherwise the kafka internal state will grow up indefinitely, which we don't want
    val paymentsStream = builder.stream[OrderId, Payment](PaymentsTopic)

    val joinWindow = JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES))
    val joinOrdersPayments = (order: Order, payment: Payment) => if (payment.status == "PAID") Option(order) else Option.empty[Order]

    val ordersPaid = ordersStream.join(paymentsStream)(joinOrdersPayments, joinWindow)
      .flatMapValues(maybeOrder => maybeOrder.toList)

    // Sink
    // We could shove the data coming from a stream through a sync and send it to a final kafka topic,
    // that we can then subscribe in another place in our distributed application
    ordersPaid.to(PaidOrdersTopic)

    // And finally we build out topology
    val topology = builder.build()

    val props = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)

//    println(topology.describe())

    val application = new KafkaStreams(topology, props)
    application.start()

  }
}
