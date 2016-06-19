package dynamite

import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.{ ConsumedCapacity, ScalarAttributeType }
import scala.util.Try

sealed abstract class Response

object Response {
  final case class ResultSet(
    results: Iterator[Timed[Try[List[Item]]]],
    capacity: Option[() => ConsumedCapacity] = None
  ) extends Response

  final case class TableNames(names: Iterator[Timed[Try[List[String]]]])
    extends Response

  case class PrimaryKey(name: String)

  final case class TableDescription(
    name: String,
    hash: KeySchema,
    range: Option[KeySchema],
    indexes: Seq[Index]
  ) extends Response

  case object Complete extends Response

  final case class KeySchema(name: String, `type`: ScalarAttributeType)

  final case class Index(
    name: String,
    hash: KeySchema,
    range: Option[KeySchema]
  )

}
