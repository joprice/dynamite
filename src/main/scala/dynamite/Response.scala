package dynamite

import com.amazonaws.services.dynamodbv2.model.{
  ConsumedCapacity,
  ScalarAttributeType
}
import dynamite.Dynamo.DynamoObject

import scala.util.Try

sealed abstract class Response

object Response {
  final case class Info(message: String) extends Response

  final case class ResultSet(
      results: Iterator[Timed[Try[List[DynamoObject]]]],
      capacity: Option[() => ConsumedCapacity] = None
  ) extends Response

  final case class TableNames(names: Iterator[Timed[Try[List[String]]]])
      extends Response

  sealed trait TableSchema {
    def name: String
    def hash: KeySchema
    def range: Option[KeySchema]
  }

  final case class TableDescription(
      name: String,
      hash: KeySchema,
      range: Option[KeySchema],
      indexes: Seq[Index]
  ) extends Response
      with TableSchema

  final case class Index(
      name: String,
      hash: KeySchema,
      range: Option[KeySchema]
  ) extends TableSchema

  case object Complete extends Response

  final case class KeySchema(name: String, `type`: ScalarAttributeType)

}
