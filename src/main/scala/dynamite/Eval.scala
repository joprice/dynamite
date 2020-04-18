package dynamite

import com.amazonaws.services.dynamodbv2.document.{
  PrimaryKey => DynamoPrimaryKey,
  Index => _,
  _
}
import dynamite.Ast.{PrimaryKey => _, _}
import com.amazonaws.services.dynamodbv2.model.{
  Select => _,
  Delete => _,
  TableDescription => _,
  Update => _,
  _
}
import dynamite.Ast.Projection.{Aggregate, FieldSelector}
import dynamite.Response._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.handlers.AsyncHandler
import zio._

object Eval {

  def apply(
      dynamo: AmazonDynamoDBAsync,
      query: Query,
      pageSize: Int,
      tableCache: TableCache
  ): Task[Response] = {
    query match {
      case query: Select => select(dynamo, query, pageSize, tableCache)
      case query: Update => update(dynamo, query)
      case query: Delete => delete(dynamo, query)
      case query: Insert => insert(dynamo, query)
      case ShowTables    => showTables(dynamo)
      case DescribeTable(table) =>
        tableCache
          .get(table)
          .map(_.getOrElse(Response.Info(s"No table exists with name $table")))
    }
  }

  def assumeIndex(
      tableCache: TableCache,
      tableName: String,
      hashKey: String,
      rangeKey: Option[String],
      orderBy: Option[OrderBy]
  ): Task[Option[Index]] =
    for {
      table <- tableCache
        .get(tableName)
        .fold(
          error => throw error,
          table => table.getOrElse(throw UnknownTableException(tableName))
        )
      result <- {
        val grouped = table.indexes.groupBy(_.hash.name)
        grouped
          .get(hashKey)
          .map { candidates =>
            val results = rangeKey.fold(candidates) { range =>
              candidates.filter(_.range.exists(_.name == range))
            }
            val filtered = orderBy.fold(results) { order =>
              results.filter { result =>
                result.range
                  .fold(order.field == result.hash.name)(_.name == order.field)
              }
            }
            filtered match {
              case Seq(index) => Task.succeed(Some(index))
              case Seq()      => Task.succeed(None)
              case indexes =>
                Task.fail(AmbiguousIndexException(indexes.map(_.name)))
            }
          }
          .getOrElse(Task.succeed(None))
      }
    } yield result

  def validateOrderBy(
      hash: KeySchema,
      range: Option[KeySchema],
      orderBy: Option[OrderBy]
  ) = {
    orderBy.foreach { order =>
      val validOrder =
        range.fold(order.field == hash.name)(_.name == order.field)
      if (!validOrder) {
        throw new Exception(s"'${order.field}' is not a valid sort field.")
      }
    }
  }

  def validateIndex(
      tableDescription: TableDescription,
      indexName: String,
      hashKey: String,
      rangeKey: Option[String]
  ): Task[Index] = {
    tableDescription.indexes
      .find(_.name == indexName)
      .map { index =>
        if (index.hash.name != hashKey) {
          Task.fail(InvalidHashKeyException(index, hashKey))
        } else {
          (rangeKey, index.range) match {
            case (Some(left), Some(right)) if left != right.name =>
              Task.fail(InvalidRangeKeyException(index.name, right, left))
            case (Some(left), None) =>
              Task.fail(UnexpectedRangeKeyException(left))
            case (None, Some(right)) =>
              Task.fail(MissingRangeKeyException(right.name))
            case _ => Task.succeed(index)
          }
        }
      }
      .getOrElse {
        Task.fail(UnknownIndexException(indexName))
      }
  }

  //TODO: handle pagination - use withMaxPageSize instead?
  def scanForward(dir: Option[Direction]) = dir match {
    case Some(Ascending)  => true
    case Some(Descending) => false
    case None             => true
  }

  def toDynamoKey(key: Ast.PrimaryKey) = {
    val Ast.PrimaryKey(hash, range) = key
    def toAttr(key: Ast.Key) = new KeyAttribute(key.field, unwrap(key.value))
    val attrs = toAttr(hash) +: range.map(toAttr).toSeq
    new DynamoPrimaryKey(attrs: _*)
  }

//  def toAliasedKey(
//      key: Ast.PrimaryKey
//  ): java.util.Map[String, AttributeValue] = {
//    val Ast.PrimaryKey(hash, range) = key
//    def aliasValue(key: Ast.Key) =
//      Map(s":${key.field}" -> toAttributeValue(key.value))
//    def toAttr(key: Ast.Key) = Map(s"#${key.field}" -> s":${key.field}")
//
//    val primary = toAttr(hash)
//    val primaryValue = aliasValue(hash)
//    (
//      range.map(toAttr).fold(primary)(primary ++ _).asJava,
//      range.map(aliasValue).fold(primaryValue)(primaryValue ++ _).asJava
//    )
//  }

  def toKey(key: Ast.PrimaryKey): java.util.Map[String, AttributeValue] = {
    val Ast.PrimaryKey(hash, range) = key
    def toAttr(key: Ast.Key) = Map(key.field -> toAttributeValue(key.value))
    val primary = toAttr(hash)
    range.map(toAttr).fold(primary)(primary ++ _).asJava
  }

  def toAttributeValue(value: Value): AttributeValue = value match {
    case BoolValue(value)   => new AttributeValue().withBOOL(value)
    case StringValue(value) => new AttributeValue().withS(value)
    case number: NumberValue =>
      new AttributeValue().withN(number.value.toString)
    case ObjectValue(values) =>
      val map: Map[String, AttributeValue] = values.map {
        case (key, value) =>
          key -> toAttributeValue(value)
      }.toMap
      new AttributeValue().withM(map.asJava)
    case ListValue(value) =>
      new AttributeValue().withL(value.map(toAttributeValue).asJava)
  }

  def unwrap(value: Value): AnyRef = value match {
    case BoolValue(value)    => value: java.lang.Boolean
    case StringValue(value)  => value
    case number: NumberValue => number.value
    case ObjectValue(values) =>
      val map: Map[String, AnyRef] = values.map {
        case (key, value) =>
          key -> unwrap(value)
      }.toMap
      map.asJava
    case ListValue(value) => value.map(unwrap).asJava
  }

  def resolveProjection(
      projection: Seq[Ast.Projection]
  ): (Seq[Aggregate], Seq[String]) = {
    def loop(projection: Seq[Ast.Projection]): (Seq[Aggregate], Seq[String]) = {
      val fields =
        projection.foldLeft((Seq.empty: Seq[Aggregate], Vector.empty[String])) {
          case (state, FieldSelector.All) => state
          case ((aggregates, fields), FieldSelector.Field(field)) =>
            (aggregates, fields :+ field)
          case ((aggregates, fields), count: Aggregate.Count.type) =>
            (aggregates :+ count, fields)
        }
      fields
    }
    //TODO: need to dedupe?
    val (aggregates, fields) = loop(projection)
    (aggregates, fields.distinct)
  }

  def applyAggregates(
      aggregates: Seq[Aggregate],
      resultSet: ResultSet
  ): ResultSet = {
    // For now, using headOption, since the only aggregate supported at the moment is Count(*) and duplicate
    // aggregates will be resolved by reference
    aggregates.headOption.fold(resultSet) {
      case agg: Aggregate.Count.type =>
        resultSet.copy(
          results = LazySingleIterator {
            val it = resultSet.results
            var totalDuration = 0.seconds
            var count = 0
            var failure: Option[Throwable] = None
            while (failure.isEmpty && it.hasNext) {
              val Timed(result, duration) = it.next
              result match {
                case Success(rows) =>
                  count += rows.size
                case Failure(ex) => failure = Some(ex)
              }
              totalDuration += duration
            }

            Timed(
              failure
                .map[Try[List[Map[String, AttributeValue]]]] { ex =>
                  Failure[List[Map[String, AttributeValue]]](ex)
                }
                .getOrElse {
                  Success(
                    List(
                      Map(
                        agg.name -> toAttributeValue(
                          Ast.IntValue(count.toString)
                        )
                      )
                    )
                  )
                },
              totalDuration
            )
          }
        )
    }
  }

  //TODO: use UnknownTableException
  def recoverQuery[A](table: String, result: Task[A]) = {
    result.mapError {
      case _: ResourceNotFoundException =>
        new Exception(s"Table '$table' does not exist")
      case other => other
    }
  }

  final case class AmbiguousIndexException(indexes: Seq[String])
      extends Exception(
        s"""Multiple indexes can fulfill the query: ${indexes.mkString(
             "[",
             ", ",
             "]"
           )}.
       |Choose an index explicitly with a 'use index' clause.""".stripMargin
      )

  final case class UnknownTableException(table: String)
      extends Exception(
        s"Unknown table: $table"
      )

  final case class UnknownIndexException(index: String)
      extends Exception(
        s"Unknown index: $index"
      )

  final case class InvalidHashKeyException(index: Index, hashKey: String)
      extends Exception(
        s"Hash key of '${index.hash.name}' of index '${index.name}' does not match provided hash key '$hashKey'."
      )

  final case class MissingRangeKeyException(rangeKey: String)
      extends Exception(
        s"Missing range key $rangeKey"
      )

  final case class UnexpectedRangeKeyException(rangeKey: String)
      extends Exception(
        s"Unexpected range key $rangeKey"
      )

  final case class InvalidRangeKeyException(
      indexName: String,
      range: KeySchema,
      rangeKey: String
  ) extends Exception(
        s"Range key of '${range.name}' of index '${indexName}' does not match provided hash key '$rangeKey'."
      )

  object LazySingleIterator {
    def apply[A](elem: => A) = new LazySingleIterator(elem)
  }

  class LazySingleIterator[A](elem: => A) extends Iterator[A] {
    private[this] var _hasNext = true

    def hasNext: Boolean = _hasNext

    def next(): A =
      if (_hasNext) {
        _hasNext = false; elem
      } else Iterator.empty.next()
  }

  class TimedRecoveringIterator[A](
      original: Iterator[A]
  ) extends Iterator[Timed[Try[A]]] {
    private[this] val recovering = new RecoveringIterator[A](original)

    def next() = Timed {
      recovering.next()
    }

    def hasNext = recovering.hasNext

  }

  class TableIterator[A](
      select: Select,
      original: Iterator[Iterator[Map[String, AttributeValue]]]
  ) extends Iterator[Timed[Try[List[Map[String, AttributeValue]]]]] {
//    private[this] val recovering = new RecoveringIterator(original)

    def next() = Timed {
      Try {
        original.next().toList
      }
//      val result = recovering.next().map(_.toList)
//      Eval.recoverQuery(select.from, result)
    }

    def hasNext = original.hasNext

  }

  def describeTable(
      dynamo: AmazonDynamoDBAsync,
      tableName: String
  ): Task[TableDescription] =
    for {
      result <- Dynamo.dynamoRequest(
        dynamo.describeTableAsync(
          _: DescribeTableRequest,
          _: AsyncHandler[DescribeTableRequest, DescribeTableResult]
        ),
        new DescribeTableRequest()
          .withTableName(tableName)
      )
    } yield {
      val description = result.getTable
      val attributes =
        description.getAttributeDefinitions.asScala.map { definition =>
          definition.getAttributeName -> definition.getAttributeType
        }.toMap

      def getKey(elements: Seq[KeySchemaElement], keyType: KeyType) =
        elements.find(_.getKeyType == keyType.toString)

      def keySchema(elements: Seq[KeySchemaElement], keyType: KeyType) =
        for {
          key <- getKey(elements, keyType)
          name = key.getAttributeName
          tpe <- attributes.get(name)
        } yield KeySchema(name, ScalarAttributeType.fromValue(tpe))

      def indexSchema(name: String, elements: Seq[KeySchemaElement]) =
        for {
          hash <- keySchema(elements, KeyType.HASH)
        } yield {
          Index(name, hash, keySchema(elements, KeyType.RANGE))
        }

      // even Local and Global secondary index descriptors have almost the same structure,
      // they do not share a common interface, so this code is repeated twice

      val globalIndexes = Option(description.getGlobalSecondaryIndexes)
        .map(_.asScala.map { index =>
          (index.getIndexName, index.getKeySchema.asScala)
        })
        .getOrElse(Seq.empty)

      val localIndexes = Option(description.getLocalSecondaryIndexes)
        .map(_.asScala.map {
          //TODO: get projection as well for autocompletion?
          index => (index.getIndexName, index.getKeySchema.asScala)
        })
        .getOrElse(Seq.empty)

      val schemas = (globalIndexes ++ localIndexes).flatMap {
        case (name, keys) => indexSchema(name, keys.toVector)
      }.toVector

      val elements = description.getKeySchema.asScala.toVector
      val key = keySchema(elements, KeyType.HASH)
      val range = keySchema(elements, KeyType.RANGE)

      TableDescription(tableName, key.get, range, schemas)
    }

  def showTables(dynamo: AmazonDynamoDBAsync): Task[TableNames] = {
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
        _.getTableNames()
      }
      .map { it =>
        TableNames(new TimedRecoveringIterator(Iterator(it.asScala.toList)))
      }
  }

  def insert(
      dynamo: AmazonDynamoDBAsync,
      query: Insert
  ): Task[Complete.type] = {
    val item = query.values
      .foldLeft(Map[String, AttributeValue]()) {
        case (item, (field, value)) =>
          item.updated(field, toAttributeValue(value))
      }
      .asJava
    Dynamo
      .dynamoRequest(
        dynamo.putItemAsync,
        new PutItemRequest()
          .withTableName(query.table)
          .withItem(item)
      )
      .as(Complete)
  }

  //TODO: condition expression that item must exist?
  def delete(dynamo: AmazonDynamoDBAsync, query: Delete): Task[Complete.type] =
    //TODO: optionally return consumed capacity
    //.getConsumedCapacity
    Dynamo
      .dynamoRequest(
        dynamo.deleteItemAsync,
        new DeleteItemRequest()
          .withTableName(query.table)
          .withKey(toKey(query.key))
      )
      .as(Complete)

  //TODO: change response to more informative type
  def update(
      dynamo: AmazonDynamoDBAsync,
      query: Update
  ): Task[Complete.type] = {
    recoverQuery(
      query.table, {
        val baseSpec = new UpdateItemRequest()
          .withTableName(query.table)
//        val expected = (Seq(hash) ++ range.toSeq)
//          .map(key => key -> new ExpectedAttributeValue(key.field).exists())
//          .toMap
        val spec = baseSpec
          .withKey(toKey(query.key))
          .withUpdateExpression(
            "set " +
              query.fields
                .map {
                  case (key, _) =>
                    s"#$key = :$key"
                }
                .mkString(", ")
          )
          .withExpressionAttributeNames(
            query.fields
              .map {
                case (key, _) =>
                  s"#$key" -> key
              }
              .toMap
              .asJava
          )
          .withExpressionAttributeValues(
            query.fields
              .map {
                case (key, value) =>
                  s":$key" -> toAttributeValue(value)
              }
              .toMap
              .asJava
          )
        // .withExpected(expected.asJava)
        Dynamo
          .dynamoRequest(dynamo.updateItemAsync, spec)
          .as(Complete)
      }
    )
  }

  def select(
      dynamo: AmazonDynamoDBAsync,
      query: Select,
      pageSize: Int,
      tableCache: TableCache
  ): Task[ResultSet] = {
    //TODO stream of pages
    def wrap[A](
        data: java.util.List[java.util.Map[String, AttributeValue]],
        capacity: Option[ConsumedCapacity]
    ) = {
      /*
      val it = results.pages.iterator()
      val original: Iterator[Iterator[JsonNode]] = it.asScala.map {
        _.asScala.map(Eval.jsonNode).iterator
      }
      ResultSet(
        new TableIterator(query, original),
        Some(() => results.getAccumulatedConsumedCapacity)
      )
       */
      val original: Iterator[Iterator[Map[String, AttributeValue]]] = Iterator {
        data.asScala.iterator.map(_.asScala.toMap)
      }
//      Some(() => results.getAccumulatedConsumedCapacity)
      ResultSet(
        new TableIterator(query, original),
        capacity.map(value => () => value)
        //Some(() => results.get  AccumulatedConsumedCapacity)
      )
    }

    def querySpec(keyFields: Seq[String]) = {
      val spec = new QueryRequest()
        .withReturnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        .withTableName(query.from)

      val (aggregates, fields) = resolveProjection(query.projection)
      val allFields = keyFields ++ fields
      def toKey(field: String) = s"#$field"
      val fieldsKeys = fields.map(toKey)
      val keys = keyFields.map(toKey) ++ fieldsKeys
      val withFields = if (fields.nonEmpty) {
        spec
          .withProjectionExpression(fieldsKeys.mkString(","))
      } else spec
      val withProjection = if (allFields.nonEmpty) {
        withFields.withExpressionAttributeNames(
          keys.zip(allFields).distinct.toMap.asJava
        )
      } else withFields

      (
        aggregates,
        query.limit
          .fold(withProjection) { limit => withProjection.withLimit(limit) }
          // .withMaxPageSize(pageSize)
          .withScanIndexForward(
            scanForward(query.orderBy.flatMap(_.direction))
          )
      )
    }

    def querySingle(
        tableDescription: TableDescription,
        hash: Key,
        range: Key
    ) = {
      val Key(hashKey, hashValue) = hash
      val Key(rangeKey, rangeValue) = range

      def validateKeys(schema: TableSchema): Unit = {
        if (hashKey != schema.hash.name) {
          throw new Exception(s"Invalid hash key '$hashKey'")
        }
        schema.range match {
          case Some(range) =>
            if (rangeKey != range.name) {
              throw new Exception(s"Invalid range key '$rangeKey'")
            }
          case None =>
            throw new Exception(
              s"Schema '${schema.name}' does not contain a range key"
            )
        }
      }

      // Because indexes can have duplicates, GetItemSpec cannot be used. Query must be used instead.
      def queryIndex(index: Index) = {
        validateKeys(index)

        val (aggregates, spec) = querySpec(Seq(hashKey, rangeKey))
        val results =
          Dynamo.dynamoRequest(
            dynamo.queryAsync,
            spec
              .withTableName(query.from)
              .withIndexName(index.name)
              .withKeyConditionExpression(
                s"#$hashKey = :$hashKey and #$rangeKey= :$rangeKey"
              )
              .withExpressionAttributeValues(
                Map(
                  s":$hashKey" -> toAttributeValue(hashValue),
                  s":$rangeKey" -> toAttributeValue(rangeValue)
                ).asJava
              )
            //TODO: support order by on index queries?
            //.withScanIndexForward()
          )
        results.map { data =>
          (aggregates, wrap(data.getItems, Option(data.getConsumedCapacity)))
        }
      }

      def getItem(tableDescription: TableDescription) = {
        validateKeys(tableDescription)

        val spec = new GetItemRequest()
          .withTableName(query.from)
        val withKey = spec.withKey(toKey(Ast.PrimaryKey(hash, Some(range))))
        val (aggregates, fields) = resolveProjection(query.projection)
        val withFields = if (fields.nonEmpty) {
          val keys = fields.map { field => s"#$field" }
          withKey
            .withProjectionExpression(keys.mkString(","))
            .withExpressionAttributeNames(
              keys.zip(fields).toMap.asJava
            )
        } else spec

        Dynamo.dynamoRequest(dynamo.getItemAsync, withFields).map { data =>
          val results = ResultSet(
            Iterator(
              Timed(
                Try(Option(data.getItem).toList.map(_.asScala.toMap))
              )
            )
          )
          (aggregates, results)
        }
      }

      val getIndex = query.useIndex
        .map(value =>
          validateIndex(
            tableDescription,
            value,
            hash.field,
            Some(range.field)
          ).map(Option.apply)
        )
        .getOrElse(
          assumeIndex(
            tableCache,
            tableName = query.from,
            hashKey = hashKey,
            rangeKey = Some(rangeKey),
            orderBy = query.orderBy
          )
        )

      for {
        index <- getIndex
        result <- index
          .map { index =>
            //TODO: check types as well?
            validateOrderBy(index.hash, index.range, query.orderBy)
            queryIndex(index)
          }
          .getOrElse(getItem(tableDescription))
      } yield result
    }

    def range(
        tableDescription: TableDescription,
        field: String,
        value: KeyValue
    ) = {
      val (aggregates, spec) = querySpec(Seq(field))

      val results: ZIO[Any, Throwable, QueryResult] = query.useIndex
        .map(value =>
          validateIndex(tableDescription, value, field, None).map(Option.apply)
        )
        .getOrElse {
          def sortByTableField = query.orderBy.forall { order =>
            order.field == tableDescription.hash.name || tableDescription.range
              .exists(_.name == field)
          }
          // If the main table has the correct hash key, use that. Otherwise, check indexes. This is to avoid the
          // addition of a single local secondary index forcing all queries to add a 'use index' clause.
          if (tableDescription.hash.name == field && sortByTableField) {
            Task.succeed(Option.empty[Index])
          } else {
            assumeIndex(
              tableCache,
              tableName = query.from,
              hashKey = field,
              rangeKey = None,
              orderBy = query.orderBy
            )
          }
        }
        .flatMap { maybeIndex =>
          maybeIndex
            .map { index =>
              validateOrderBy(index.hash, index.range, query.orderBy)
              //toAliasedKey(Ast.PrimaryKey(Key(index.hash, index.range))
              Dynamo.dynamoRequest(
                dynamo.queryAsync,
                spec
                  .withKeyConditionExpression(
                    s"#$field = :$field"
                  )
                  .withExpressionAttributeValues(
                    Map(
                      s":$field" -> toAttributeValue(value)
                    ).asJava
                  )
                  .withIndexName(index.name)
              )
            }
            .getOrElse {
              validateOrderBy(
                tableDescription.hash,
                tableDescription.range,
                query.orderBy
              )
              Dynamo
                .dynamoRequest(
                  dynamo.queryAsync,
                  spec
                    .withKeyConditionExpression(
                      s"#$field = :$field"
                    )
                    .withExpressionAttributeValues(
                      Map(
                        s":$field" -> toAttributeValue(value)
                      ).asJava
                    )
                )
            }
        }
      results.map { data =>
        (aggregates, wrap(data.getItems, Option(data.getConsumedCapacity)))
      }
    }

    def scan() = {
      //TODO: scan doesn't support order (because doesn't on hash key?)
      val spec = new ScanRequest()
        .withTableName(query.from)
      val (aggregates, fields) = resolveProjection(query.projection)
      val withFields = if (fields.nonEmpty) {
        val keys = fields.map { field => s"#$field" }
        spec
          .withProjectionExpression(keys.mkString(","))
          .withExpressionAttributeNames(
            keys.zip(fields).toMap.asJava
          )
      } else spec
      val withLimit = query.limit
        .fold(withFields) { limit => withFields.withLimit(limit) }
      val results = Dynamo.dynamoRequest(dynamo.scanAsync, withLimit)
      results.map(value =>
        (aggregates, wrap(value.getItems, Option(value.getConsumedCapacity)))
      )
    }
    for {
      //TODO: return instead of throwing
      tableDescription <- tableCache
        .get(query.from)
        .foldM(
          {
            case _: ResourceNotFoundException =>
              Task.fail(UnknownTableException(query.from))
            case error => Task.fail(error)
          },
          table =>
            table.fold[Task[TableDescription]](
              Task.fail(UnknownTableException(query.from))
            ) {
              Task.succeed(_)
            }
        )
      //TODO: abstract over GetItemSpec, QuerySpec and ScanSpec?
      result <- query.where match {
        case Some(Ast.PrimaryKey(hash, Some(range))) =>
          querySingle(tableDescription, hash = hash, range = range)
        case Some(Ast.PrimaryKey(Ast.Key(field, value), None)) =>
          range(tableDescription, field, value)
        case None =>
          if (query.orderBy.isDefined) {
            throw new Exception(
              "'order by' is not currently supported on scan queries"
            )
          }
          scan()
      }
    } yield {
      val (aggregates, results) = result
      applyAggregates(aggregates, results)
    }

  }
}
