package dynamite

import zio.stream.ZStream
import com.amazonaws.services.dynamodbv2.model.{
  ConsumedCapacity,
  ScalarAttributeType
}
import dynamite.Dynamo.DynamoObject
import zio.{RIO, Task}

sealed abstract class Response

object Response {

  type Paged[R, A] = ZStream[R, Throwable, Timed[A]]
  type ResultPage = Paged[Eval.Env, PageType]

  final case class Info(message: String) extends Response

  final case class ResultSet(
      results: Paged[Eval.Env, List[DynamoObject]],
      capacity: RIO[Eval.Env, Option[() => ConsumedCapacity]] =
        Task.succeed(None)
  ) extends Response

  final case class TableNames(
      names: Paged[Eval.Env, List[String]]
  ) extends Response

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
