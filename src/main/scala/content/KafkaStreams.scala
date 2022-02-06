package content

import io.circe.{Decoder, Encoder}
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.ImplicitConversions._


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


  builder.build()


  def main(args: Array[String]): Unit = {
  }
}
