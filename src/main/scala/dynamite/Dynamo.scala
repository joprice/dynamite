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
import com.amazonaws.services.dynamodbv2.model.{
  Delete => _,
  Select => _,
  TableDescription => _,
  Update => _,
  _
}

object Dynamo {
  type DynamoObject = Map[String, AttributeValue]

  // taken from Scanamo
  final private def dynamoRequest[A <: AmazonWebServiceRequest, B](
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

  def describeTable(
      dynamo: AmazonDynamoDBAsync,
      tableName: String
  ) =
    Dynamo.dynamoRequest(
      dynamo.describeTableAsync(
        _: DescribeTableRequest,
        _: AsyncHandler[DescribeTableRequest, DescribeTableResult]
      ),
      new DescribeTableRequest()
        .withTableName(tableName)
    )

  def listTables(
      dynamo: AmazonDynamoDBAsync
  ) =
    Dynamo
      .dynamoRequest(
        dynamo.listTablesAsync(
          _: ListTablesRequest,
          _: AsyncHandler[ListTablesRequest, ListTablesResult]
        ),
        new ListTablesRequest()
      )
      // //TODO: time by page, and sum?
      // def it = dynamo.listTables().pages().iterator().asScala.map {
      //   _.iterator().asScala.map(_.getTableName).toList
      // }
      .map {
        _.getTableNames.asScala.toList
      }

  def scan(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      fields: Seq[String],
      limit: Option[Int]
  ) = {
    val spec = new ScanRequest()
      .withTableName(tableName)
    val withFields = if (fields.nonEmpty) {
      val keys = fields.map(field => s"#$field")
      spec
        .withProjectionExpression(keys.mkString(","))
        .withExpressionAttributeNames(
          keys.zip(fields).toMap.asJava
        )
    } else spec
    val withLimit = limit
      .fold(withFields)(limit => withFields.withLimit(limit))
    Dynamo.dynamoRequest(dynamo.scanAsync, withLimit)
  }

  def insert(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      item: DynamoObject
  ) =
    Dynamo
      .dynamoRequest(
        dynamo.putItemAsync,
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(item.asJava)
      )

  type Key = (String, AttributeValue)

  implicit class OptionChain[A](val value: A) extends AnyVal {
    def tapOpt[B, C](opt: Option[B])(orElse: A => C)(f: (A, B) => C): C =
      opt.fold(orElse(value))(f(value, _))
  }

  def query(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Option[Key],
      index: Option[String],
      fields: Seq[String],
      limit: Option[Int],
      scanForward: Boolean
  ) = {
    val (hashKey, hashValue) = hash
    val keyFields = hashKey +: range.map(_._1).toList
    val spec = {
      val _spec = new QueryRequest()
        .withReturnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        .withTableName(tableName)
      val allFields = keyFields ++ fields
      def toKey(field: String) = s"#$field"
      val fieldsKeys = fields.map(toKey)
      val keys = keyFields.map(toKey) ++ fieldsKeys
      val withFields = if (fields.nonEmpty) {
        _spec
          .withProjectionExpression(fieldsKeys.mkString(","))
      } else _spec
      val withProjection = if (allFields.nonEmpty) {
        withFields.withExpressionAttributeNames(
          keys.zip(allFields).distinct.toMap.asJava
        )
      } else withFields
      val withLimit = limit
        .fold(withProjection)(limit => withProjection.withLimit(limit))
        // .withMaxPageSize(pageSize)
        .withScanIndexForward(scanForward)
      index
        .fold(withLimit)(index => withLimit.withIndexName(index))
        .tapOpt(range)(
          _.withKeyConditionExpression(
            s"#$hashKey = :$hashKey"
          ).withExpressionAttributeValues(
            Map(
              s":$hashKey" -> hashValue
            ).asJava
          )
        ) {
          case (spec, (rangeKey, rangeValue)) =>
            spec
              .withKeyConditionExpression(
                s"#$hashKey = :$hashKey and #$rangeKey= :$rangeKey"
              )
              .withExpressionAttributeValues(
                Map(
                  s":$hashKey" -> hashValue,
                  s":$rangeKey" -> rangeValue
                ).asJava
              )
        }
    }
    dynamoRequest(dynamo.queryAsync, spec)
  }

  def getItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Key,
      fields: Seq[String]
  ) = {
    val spec = {
      val _spec = new GetItemRequest()
        .withTableName(tableName)
      val withKey = _spec.withKey(Map(hash, range).asJava)
      if (fields.nonEmpty) {
        val keys = fields.map(field => s"#$field")
        withKey
          .withProjectionExpression(keys.mkString(","))
          .withExpressionAttributeNames(
            keys.zip(fields).toMap.asJava
          )
      } else _spec
    }
    dynamoRequest(dynamo.getItemAsync, spec)
  }

  def putItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      item: DynamoObject
  ) =
    Dynamo
      .dynamoRequest(
        dynamo.putItemAsync,
        new PutItemRequest()
          .withTableName(tableName)
          .withItem(item.asJava)
      )

  def deleteItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Option[Key]
  ) =
    dynamoRequest(
      dynamo.deleteItemAsync,
      new DeleteItemRequest()
        .withTableName(tableName)
        .withKey(
          (Map(hash) ++ range.fold[DynamoObject](Map.empty)(Map(_))).asJava
        )
    )

  def updateItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Option[Key],
      fields: Seq[(String, AttributeValue)]
  ) = {
    //TODO: avoid accidental upsert on update
    // .withExpected(expected.asJava)
    //        val expected = (Seq(hash) ++ range.toSeq)
    // .map(key => key -> new ExpectedAttributeValue(key.field).exists())
    // .toMap
    val spec = new UpdateItemRequest()
      .withTableName(tableName)
      .withKey(
        (Map(hash) ++ range.fold[DynamoObject](Map.empty)(Map(_))).asJava
      )
      .withUpdateExpression(
        "set " +
          fields
            .map {
              case (key, _) =>
                s"#$key = :$key"
            }
            .mkString(", ")
      )
      .withExpressionAttributeNames(
        fields
          .map {
            case (key, _) =>
              s"#$key" -> key
          }
          .toMap
          .asJava
      )
      .withExpressionAttributeValues(
        fields
          .map {
            case (key, value) =>
              s":$key" -> value
          }
          .toMap
          .asJava
      )
    // .withExpected(expected.asJava)
    dynamoRequest(dynamo.updateItemAsync, spec)
  }
}
