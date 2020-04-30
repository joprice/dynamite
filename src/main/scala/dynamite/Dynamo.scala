package dynamite

import scala.concurrent.duration._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.{
  AmazonWebServiceRequest,
  ClientConfiguration,
  SdkClientException
}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.dynamodbv2.{
  AmazonDynamoDBAsync,
  AmazonDynamoDBAsyncClientBuilder
}
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
import org.apache.http.conn.HttpHostConnectException
import zio.config.Config
import zio.stream.ZStream

object Dynamo {
  type DynamoObject = Map[String, AttributeValue]
  type Result[A] = IO[Exception, A]
  type Dynamo = Has[Service]
  type DynamoClient = Has[AmazonDynamoDBAsync]
  type ServiceResult[A] = ZIO[Dynamo, Exception, A]
  type Key = (String, AttributeValue)

  def dynamoClient(config: DynamiteConfig, opts: Opts): AmazonDynamoDBAsync = {
    val endpoint = opts.endpoint.orElse(config.endpoint)
    val credentials = opts.profile.map {
      new ProfileCredentialsProvider(_)
    }
    dynamoClient(endpoint, credentials)
  }

  def dynamoClient(
      endpoint: Option[String],
      credentials: Option[AWSCredentialsProvider]
  ): AmazonDynamoDBAsync = {
    val clientConfig = new ClientConfiguration()
      .withConnectionTimeout(1.second.toMillis.toInt)
      .withSocketTimeout(1.second.toMillis.toInt)
    val builder = AmazonDynamoDBAsyncClientBuilder
      .standard()
      .withClientConfiguration(clientConfig)
    val withEndpoint = endpoint.fold(builder) { endpoint =>
      builder.withEndpointConfiguration(
        new EndpointConfiguration(endpoint, builder.getRegion)
      )
    }
    credentials
      .fold(withEndpoint) { credentials =>
        withEndpoint.withCredentials(credentials)
      }
      .build()
  }

  val clientLayer =
    ZLayer.fromAcquireRelease(
      for {
        opts <- ZIO.access[Has[Opts]](_.get)
        config <- ZIO.access[Config[DynamiteConfig]](_.get)
      } yield dynamoClient(config, opts)
    )(client => ZIO.effectTotal(client.shutdown()))

  trait Service {

    def createTable(
        tableName: String,
        hash: (String, ScalarAttributeType),
        range: Option[(String, ScalarAttributeType)],
        ignoreExisting: Boolean
    ): Result[Unit]

    def describeTable(
        tableName: String
    ): Result[DescribeTableResult]

    def listTables: zio.stream.Stream[Exception, List[String]]

    def scan(
        tableName: String,
        fields: Seq[String],
        limit: Option[Int]
    ): ZStream[Dynamo, Exception, (List[DynamoObject], Boolean)]

    def insert(
        tableName: String,
        item: DynamoObject
    ): Result[PutItemResult]

    def query(
        tableName: String,
        hash: Key,
        range: Option[Key],
        index: Option[String],
        fields: Seq[String],
        limit: Option[Int],
        scanForward: Boolean
    ): Result[QueryResult]

    def getItem(
        tableName: String,
        hash: Key,
        range: Key,
        fields: Seq[String]
    ): Result[GetItemResult]

    def putItem(
        tableName: String,
        item: DynamoObject
    ): Result[PutItemResult]

    def deleteItem(
        tableName: String,
        hash: Key,
        range: Option[Key]
    ): Result[DeleteItemResult]

    def updateItem(
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
  ): IO[Exception, B] =
    IO.effectAsync[Exception, B] { cb =>
      val handler = new AsyncHandler[A, B] {
        def onError(exception: Exception): Unit =
          exception match {
            case e: AmazonDynamoDBException => cb(IO.fail(e))
            case e: SdkClientException =>
              cb(
                Option(e.getCause)
                  .collect {
                    case ex: HttpHostConnectException =>
                      IO.fail(ex)
                  }
                  .getOrElse(IO.die(e))
              )
            case t => cb(IO.die(t))
          }

        def onSuccess(request: A, result: B): Unit =
          cb(IO.succeed(result))
      }
      val _ = f(req, handler)
    }

  val live = ZLayer.fromService[AmazonDynamoDBAsync, Service] { dynamo =>
    new Service {

      def describeTable(
          tableName: String
      ) =
        dynamoRequest(
          dynamo.describeTableAsync(
            _: DescribeTableRequest,
            _: AsyncHandler[DescribeTableRequest, DescribeTableResult]
          ),
          new DescribeTableRequest()
            .withTableName(tableName)
        )

      def listTables = {
        def loadPage(startTable: Option[String]) =
          Dynamo
            .dynamoRequest(
              dynamo.listTablesAsync(
                _: ListTablesRequest,
                _: AsyncHandler[ListTablesRequest, ListTablesResult]
              ),
              new ListTablesRequest()
                .tapOpt(startTable)(identity)(
                  _.withExclusiveStartTableName(_)
                )
            )

        ZStream.paginateM(Left(()): Either[Unit, String]) { value =>
          for {
            result <- value match {
              case Left(())   => loadPage(None)
              case Right(key) => loadPage(Some(key))
            }
          } yield {
            val tables = result.getTableNames.asScala.toList
            val key = Option(result.getLastEvaluatedTableName)
            (tables, key.map(Right(_)))
          }
        }
      }

      def scan(
          tableName: String,
          fields: Seq[String],
          limit: Option[Int]
      ) = {
        type PageKey = java.util.Map[String, AttributeValue]

        def loadPage(key: Option[PageKey]) = {
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
          //TODO: always paginated in repl context (vs script, where higher throughput is preferred)?
          val pageSize = 40
          val actualLimit = limit.getOrElse(pageSize)
          val withLimit = withFields
            .withLimit(actualLimit)
            .tapOpt(key)(identity)(_.withExclusiveStartKey(_))
          Dynamo.dynamoRequest(dynamo.scanAsync, withLimit)
        }

        ZStream
          .paginateM((0, Left(()): Either[Unit, PageKey])) {
            case (count, value) =>
              for {
                result <- value match {
                  case Left(())   => loadPage(None)
                  case Right(key) => loadPage(Some(key))
                }
              } yield {
                val rows = result.getItems.asScala.toList
                  .map(_.asScala.toMap)
                val newSize = count + rows.size
                val key = Option(result.getLastEvaluatedKey)
                val (data, newKey) = limit.fold((rows, key)) { limit =>
                  if (newSize >= limit) {
                    (rows.take(limit - count), None)
                  } else (rows, key)
                }
                (
                  (data, key.isDefined),
                  newKey.map(value => (newSize, Right(value)))
                )
              }
          }
      }

      def insert(
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
          } else withKey
        }
        dynamoRequest(dynamo.getItemAsync, spec)
      }

      def putItem(
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
          tableName: String,
          hash: Key,
          range: Option[Key]
      ) =
        dynamoRequest(
          dynamo.deleteItemAsync,
          new DeleteItemRequest()
            .withTableName(tableName)
            .withKey(
              (Map(hash) ++ range
                .fold[DynamoObject](Map.empty)(Map(_))).asJava
            )
        )

      def createTable(
          tableName: String,
          hash: (String, ScalarAttributeType),
          range: Option[(String, ScalarAttributeType)],
          ignoreExisting: Boolean
      ) = {
        val (schemas, attributes) = (
          (hash, KeyType.HASH) :: range.map(_ -> KeyType.RANGE).toList
        ).map {
          case ((name, tpe), keyType) =>
            val schema = new KeySchemaElement()
              .withAttributeName(name)
              .withKeyType(keyType)
            val attribute = new AttributeDefinition()
              .withAttributeName(name)
              .withAttributeType(tpe)
            (schema, attribute)
        }.unzip
        dynamoRequest(
          dynamo.createTableAsync,
          new CreateTableRequest()
            .withTableName(tableName)
            .withAttributeDefinitions(
              attributes: _*
            )
            .withKeySchema(
              schemas: _*
            )
            .withProvisionedThroughput(
              new ProvisionedThroughput()
                .withReadCapacityUnits(1L)
                .withWriteCapacityUnits(1L)
            )
        ).unit
          .catchSome {
            case _: ResourceInUseException =>
              if (ignoreExisting) ZIO.unit
              else ZIO.fail(new Exception(s"Table $tableName already exists"))
          }
      }

      def updateItem(
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
          .withKey(
            (Map(hash) ++ range.fold[DynamoObject](Map.empty)(Map(_))).asJava
          )
          .withTableName(tableName)
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
  }

  def describeTable(
      tableName: String
  ): ServiceResult[DescribeTableResult] =
    ZIO.accessM[Dynamo](_.get.describeTable(tableName))

  def listTables: ZStream[Dynamo, Exception, List[String]] =
    ZStream.accessStream[Dynamo](_.get.listTables)

  def scan(
      tableName: String,
      fields: Seq[String],
      limit: Option[Int]
  ): ZStream[Dynamo, Exception, (List[DynamoObject], Boolean)] =
    ZStream.accessStream[Dynamo](_.get.scan(tableName, fields, limit))

  def insert(
      tableName: String,
      item: DynamoObject
  ): ServiceResult[PutItemResult] =
    ZIO.accessM[Dynamo](_.get.insert(tableName, item))

  def query(
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
      tableName: String,
      hash: Key,
      range: Key,
      fields: Seq[String]
  ): ServiceResult[GetItemResult] =
    ZIO.accessM[Dynamo](_.get.getItem(tableName, hash, range, fields))

  def putItem(
      tableName: String,
      item: DynamoObject
  ): ServiceResult[PutItemResult] =
    ZIO.accessM[Dynamo](_.get.putItem(tableName, item))

  def deleteItem(
      tableName: String,
      hash: Key,
      range: Option[Key]
  ): ServiceResult[DeleteItemResult] =
    ZIO.accessM[Dynamo](_.get.deleteItem(tableName, hash, range))

  def updateItem(
      tableName: String,
      hash: Key,
      range: Option[Key],
      fields: Seq[(String, AttributeValue)]
  ): ServiceResult[UpdateItemResult] =
    ZIO.accessM[Dynamo](
      _.get.updateItem(tableName, hash, range, fields)
    )

  def createTable(
      tableName: String,
      hash: (String, ScalarAttributeType),
      range: Option[(String, ScalarAttributeType)],
      ignoreExisting: Boolean
  ): ServiceResult[Unit] =
    ZIO.accessM[Dynamo](
      _.get.createTable(tableName, hash, range, ignoreExisting)
    )
}
