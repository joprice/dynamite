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
import zio.{Has, IO, ZIO, ZLayer}
import com.amazonaws.services.dynamodbv2.model.{
  Delete => _,
  Select => _,
  TableDescription => _,
  Update => _,
  _
}

object Dynamo {
  type DynamoObject = Map[String, AttributeValue]
  type Result[A] = IO[AmazonDynamoDBException, A]
  type Dynamo = Has[Service]
  type ServiceResult[A] = ZIO[Dynamo, AmazonDynamoDBException, A]
  type Key = (String, AttributeValue)

  trait Service {

    def describeTable(
        dynamo: AmazonDynamoDBAsync,
        tableName: String
    ): Result[DescribeTableResult]

    def listTables(
        dynamo: AmazonDynamoDBAsync
    ): Result[List[String]]

    def scan(
        dynamo: AmazonDynamoDBAsync,
        tableName: String,
        fields: Seq[String],
        limit: Option[Int]
    ): Result[ScanResult]

    def insert(
        dynamo: AmazonDynamoDBAsync,
        tableName: String,
        item: DynamoObject
    ): Result[PutItemResult]

    def query(
        dynamo: AmazonDynamoDBAsync,
        tableName: String,
        hash: Key,
        range: Option[Key],
        index: Option[String],
        fields: Seq[String],
        limit: Option[Int],
        scanForward: Boolean
    ): Result[QueryResult]

    def getItem(
        dynamo: AmazonDynamoDBAsync,
        tableName: String,
        hash: Key,
        range: Key,
        fields: Seq[String]
    ): Result[GetItemResult]

    def putItem(
        dynamo: AmazonDynamoDBAsync,
        tableName: String,
        item: DynamoObject
    ): Result[PutItemResult]

    def deleteItem(
        dynamo: AmazonDynamoDBAsync,
        tableName: String,
        hash: Key,
        range: Option[Key]
    ): Result[DeleteItemResult]

    def updateItem(
        dynamo: AmazonDynamoDBAsync,
        tableName: String,
        hash: Key,
        range: Option[Key],
        fields: Seq[(String, AttributeValue)]
    ): Result[UpdateItemResult]
  }

  implicit class OptionChain[A](val value: A) extends AnyVal {
    def tapOpt[B, C](opt: Option[B])(orElse: A => C)(f: (A, B) => C): C =
      opt.fold(orElse(value))(f(value, _))
  }

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

  val live = ZLayer.succeed(new Service {

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

    //TODO: paginate using ZStream
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
  })

  def describeTable(
      dynamo: AmazonDynamoDBAsync,
      tableName: String
  ): ServiceResult[DescribeTableResult] =
    ZIO.accessM[Dynamo](_.get.describeTable(dynamo, tableName))

  def listTables(
      dynamo: AmazonDynamoDBAsync
  ): ServiceResult[List[String]] =
    ZIO.accessM[Dynamo](_.get.listTables(dynamo))

  def scan(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      fields: Seq[String],
      limit: Option[Int]
  ): ServiceResult[ScanResult] =
    ZIO.accessM[Dynamo](_.get.scan(dynamo, tableName, fields, limit))

  def insert(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      item: DynamoObject
  ): ServiceResult[PutItemResult] =
    ZIO.accessM[Dynamo](_.get.insert(dynamo, tableName, item))

  def query(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Option[Key],
      index: Option[String],
      fields: Seq[String],
      limit: Option[Int],
      scanForward: Boolean
  ): ServiceResult[QueryResult] =
    ZIO.accessM[Dynamo](
      _.get.query(
        dynamo,
        tableName,
        hash,
        range,
        index,
        fields,
        limit,
        scanForward
      )
    )

  def getItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Key,
      fields: Seq[String]
  ): ServiceResult[GetItemResult] =
    ZIO.accessM[Dynamo](_.get.getItem(dynamo, tableName, hash, range, fields))

  def putItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      item: DynamoObject
  ): ServiceResult[PutItemResult] =
    ZIO.accessM[Dynamo](_.get.putItem(dynamo, tableName, item))

  def deleteItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Option[Key]
  ): ServiceResult[DeleteItemResult] =
    ZIO.accessM[Dynamo](_.get.deleteItem(dynamo, tableName, hash, range))

  val access = ZIO.accessM[Dynamo]

  def updateItem(
      dynamo: AmazonDynamoDBAsync,
      tableName: String,
      hash: Key,
      range: Option[Key],
      fields: Seq[(String, AttributeValue)]
  ): ServiceResult[UpdateItemResult] =
    access(
      _.get.updateItem(dynamo, tableName, hash, range, fields)
    )
}
