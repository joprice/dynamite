package dynamite

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model.{
  AmazonDynamoDBException,
  AttributeValue,
  PutItemRequest
}
import scala.jdk.CollectionConverters._
import zio.IO

object Dynamo {
  type DynamoObject = Map[String, AttributeValue]

  def putItem(
      dynamo: AmazonDynamoDBAsync,
      table: String,
      item: DynamoObject
  ) =
    dynamoRequest(
      dynamo.putItemAsync,
      new PutItemRequest()
        .withTableName(table)
        .withItem(item.asJava)
    )

  // taken from Scanamo
  final def dynamoRequest[A <: AmazonWebServiceRequest, B](
      f: (A, AsyncHandler[A, B]) => java.util.concurrent.Future[B],
      req: A
  ): IO[AmazonDynamoDBException, B] =
    IO.effectAsync[AmazonDynamoDBException, B] { cb =>
      val handler = new AsyncHandler[A, B] {
        def onError(exception: Exception): Unit =
          exception match {
            case e: AmazonDynamoDBException => cb(IO.fail(e))
            case t                          => cb(IO.die(t))
          }

        def onSuccess(request: A, result: B): Unit =
          cb(IO.succeed(result))
      }
      val _ = f(req, handler)
    }

}
