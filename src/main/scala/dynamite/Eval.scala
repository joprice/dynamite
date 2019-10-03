package dynamite

import com.amazonaws.jmespath.ObjectMapperSingleton
import com.amazonaws.services.dynamodbv2.document.{ PrimaryKey => DynamoPrimaryKey, Index => _, _ }
import com.amazonaws.services.dynamodbv2.document.spec._
import dynamite.Ast.{ PrimaryKey => _, _ }
import com.amazonaws.services.dynamodbv2.model.{ Select => _, TableDescription => _, _ }
import com.amazonaws.util.json.Jackson
import com.fasterxml.jackson.databind.JsonNode
import dynamite.Ast.Projection.{ Aggregate, FieldSelector }
import dynamite.Response._

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

object Eval {

  def apply(
    dynamo: DynamoDB,
    query: Query,
    pageSize: Int,
    tableCache: TableCache
  ): Try[Response] = {
    query match {
      case query: Select => select(dynamo, query, pageSize, tableCache)
      case query: Update => update(dynamo, query)
      case query: Delete => delete(dynamo, query)
      case query: Insert => insert(dynamo, query)
      case ShowTables => showTables(dynamo)
      case DescribeTable(table) =>
        tableCache.get(table)
          .map(_.getOrElse(Response.Info(s"No table exists with name $table")))
    }
  }

  def assumeIndex(
    tableCache: TableCache,
    tableName: String,
    hashKey: String,
    rangeKey: Option[String],
    orderBy: Option[OrderBy]
  ): Option[Index] = {
    val table = tableCache.get(tableName)
      .fold(
        error => throw error,
        table => table.getOrElse(throw UnknownTableException(tableName))
      )
    val grouped = table.indexes.groupBy(_.hash.name)
    grouped.get(hashKey).flatMap { candidates =>
      val results = rangeKey.fold(candidates) { range =>
        candidates.filter(_.range.exists(_.name == range))
      }
      val filtered = orderBy.fold(results) { order =>
        results.filter { result =>
          result.range.fold(order.field == result.hash.name)(_.name == order.field)
        }
      }
      filtered match {
        case Seq() => None
        case Seq(index) => Some(index)
        case indexes => throw AmbiguousIndexException(indexes.map(_.name))
      }
    }
  }

  def validateOrderBy(hash: KeySchema, range: Option[KeySchema], orderBy: Option[OrderBy]) = {
    orderBy.foreach { order =>
      val validOrder = range.fold(order.field == hash.name)(_.name == order.field)
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
  ): Index = {
    tableDescription.indexes.find(_.name == indexName).map { index =>
      if (index.hash.name != hashKey) {
        throw InvalidHashKeyException(index, hashKey)
      } else {
        (rangeKey, index.range) match {
          case (Some(left), Some(right)) if left != right.name =>
            throw InvalidRangeKeyException(index.name, right, left)
          case (Some(left), None) =>
            throw UnexpectedRangeKeyException(left)
          case (None, Some(right)) =>
            throw MissingRangeKeyException(right.name)
          case _ => index
        }
      }
    }.getOrElse {
      throw UnknownIndexException(indexName)
    }
  }

  def jsonNode(item: Item) = Jackson.jsonNodeOf(item.toJSON)

  //TODO: handle pagination - use withMaxPageSize instead?
  def scanForward(dir: Option[Direction]) = dir match {
    case Some(Ascending) => true
    case Some(Descending) => false
    case None => true
  }

  def toDynamoKey(key: Ast.PrimaryKey) = {
    val Ast.PrimaryKey(hash, range) = key
    def toAttr(key: Ast.Key) = new KeyAttribute(key.field, unwrap(key.value))
    val attrs = toAttr(hash) +: range.map(toAttr).toSeq
    new DynamoPrimaryKey(attrs: _*)
  }

  def unwrap(value: Value): AnyRef = value match {
    case BoolValue(value) => value: java.lang.Boolean
    case StringValue(value) => value
    case number: NumberValue => number.value
    case ObjectValue(values) =>
      import scala.collection.breakOut
      val map: Map[String, AnyRef] = values.map {
        case (key, value) =>
          key -> unwrap(value)
      }(breakOut)
      map.asJava
    case ListValue(value) => value.map(unwrap).asJava
  }

  def resolveProjection(projection: Seq[Ast.Projection]): (Seq[Aggregate], Seq[String]) = {
    def loop(projection: Seq[Ast.Projection]): (Seq[Aggregate], Seq[String]) = {
      val fields = projection.foldLeft((Seq.empty: Seq[Aggregate], Vector.empty[String])) {
        case (state, FieldSelector.All) => state
        case ((aggregates, fields), FieldSelector.Field(field)) => (aggregates, fields :+ field)
        case ((aggregates, fields), count: Aggregate.Count.type) => (aggregates :+ count, fields)
      }
      fields
    }
    //TODO: need to dedupe?
    val (aggregates, fields) = loop(projection)
    (aggregates, fields.distinct)
  }

  def applyAggregates(aggregates: Seq[Aggregate], resultSet: ResultSet): ResultSet = {
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
              failure.map[Try[List[JsonNode]]] { ex =>
                Failure[List[JsonNode]](ex)
              }.getOrElse {
                val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
                node.put(agg.name, count)
                Success(List(node))
              }, totalDuration
            )
          }
        )
    }
  }

  //TODO: use UnknownTableException
  def recoverQuery[A](table: String, result: Try[A]) = {
    result.recoverWith {
      case _: ResourceNotFoundException =>
        Failure(new Exception(s"Table '$table' does not exist"))
    }
  }

  final case class AmbiguousIndexException(indexes: Seq[String]) extends Exception(
    s"""Multiple indexes can fulfill the query: ${indexes.mkString("[", ", ", "]")}.
       |Choose an index explicitly with a 'use index' clause.""".stripMargin
  )

  final case class UnknownTableException(table: String) extends Exception(
    s"Unknown table: $table"
  )

  final case class UnknownIndexException(index: String) extends Exception(
    s"Unknown index: $index"
  )

  final case class InvalidHashKeyException(index: Index, hashKey: String) extends Exception(
    s"Hash key of '${index.hash.name}' of index '${index.name}' does not match provided hash key '$hashKey'."
  )

  final case class MissingRangeKeyException(rangeKey: String) extends Exception(
    s"Missing range key $rangeKey"
  )

  final case class UnexpectedRangeKeyException(rangeKey: String) extends Exception(
    s"Unexpected range key $rangeKey"
  )

  final case class InvalidRangeKeyException(indexName: String, range: KeySchema, rangeKey: String) extends Exception(
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
    original: Iterator[Iterator[JsonNode]]
  ) extends Iterator[Timed[Try[List[JsonNode]]]] {
    private[this] val recovering = new RecoveringIterator(original)

    def next() = Timed {
      val result = recovering.next().map(_.toList)
      Eval.recoverQuery(select.from, result)
    }

    def hasNext = recovering.hasNext

  }

  def describeTable(dynamo: DynamoDB, tableName: String): Try[TableDescription] = Try {
    val description = dynamo.getTable(tableName).describe()
    val attributes = description.getAttributeDefinitions.asScala.map { definition =>
      definition.getAttributeName -> definition.getAttributeType
    }.toMap

    def getKey(elements: Seq[KeySchemaElement], keyType: KeyType) =
      elements.find(_.getKeyType == keyType.toString)

    def keySchema(elements: Seq[KeySchemaElement], keyType: KeyType) = for {
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

    val globalIndexes = Option(description.getGlobalSecondaryIndexes).map(_.asScala.map {
      index => (index.getIndexName, index.getKeySchema.asScala)
    }).getOrElse(Seq.empty)

    val localIndexes = Option(description.getLocalSecondaryIndexes).map(_.asScala.map {
      //TODO: get projection as well for autocompletion?
      index => (index.getIndexName, index.getKeySchema.asScala)
    }).getOrElse(Seq.empty)

    val schemas = (globalIndexes ++ localIndexes)
      .flatMap { case (name, keys) => indexSchema(name, keys) }

    val elements = description.getKeySchema.asScala
    val key = keySchema(elements, KeyType.HASH)
    val range = keySchema(elements, KeyType.RANGE)

    TableDescription(tableName, key.get, range, schemas)
  }

  def showTables(dynamo: DynamoDB): Try[TableNames] = Try {
    //TODO: time by page, and sum?
    def it = dynamo.listTables().pages().iterator().asScala.map {
      _.iterator().asScala.map(_.getTableName).toList
    }
    TableNames(new TimedRecoveringIterator(it))
  }

  def insert(dynamo: DynamoDB, query: Insert): Try[Complete.type] = Try {
    dynamo.getTable(query.table).putItem(
      query.values.foldLeft(new Item()) {
        case (item, (field, value)) => item.`with`(field, unwrap(value))
      }
    )
    Complete
  }

  //TODO: condition expression that item must exist?
  def delete(dynamo: DynamoDB, query: Delete): Try[Complete.type] = Try {
    dynamo.getTable(query.table).deleteItem(toDynamoKey(query.key))
    Complete
    //TODO: optionally return consumed capacity
    //.getConsumedCapacity
  }

  //TODO: change response to more informative type
  def update(dynamo: DynamoDB, query: Update): Try[Complete.type] = {
    recoverQuery(query.table, Try {
      val table = dynamo.getTable(query.table)
      val Ast.PrimaryKey(hash, range) = query.key
      val baseSpec = new UpdateItemSpec()
      val expected = (Seq(hash) ++ range.toSeq)
        .map(key => new Expected(key.field).exists())
      val spec = range.fold(
        baseSpec.withPrimaryKey(hash.field, unwrap(hash.value))
      ) { range =>
          baseSpec.withPrimaryKey(
            range.field,
            unwrap(range.value),
            hash.field,
            unwrap(hash.value)
          )
        }
        .withAttributeUpdate(query.fields.map {
          case (key, value) => new AttributeUpdate(key).put(unwrap(value))
        }: _*)
        .withExpected(expected.asJava)
      table.updateItem(spec)
      Complete
    })
  }

  def select(dynamo: DynamoDB, query: Select, pageSize: Int, tableCache: TableCache): Try[ResultSet] = Try {
    //TODO: return instead of throwing
    lazy val tableDescription = tableCache.get(query.from).fold(
      {
        case _: ResourceNotFoundException => throw UnknownTableException(query.from)
        case error => throw error
      },
      table => table.getOrElse(throw UnknownTableException(query.from))
    )

    val table = dynamo.getTable(query.from)

    def wrap[A](results: ItemCollection[A]) = {
      import scala.collection.breakOut
      val it = results.pages.iterator()
      val original: Iterator[Iterator[JsonNode]] = it.asScala.map {
        _.asScala.map(Eval.jsonNode)(breakOut)
      }
      ResultSet(
        new TableIterator(query, original),
        Some(() => results.getAccumulatedConsumedCapacity)
      )
    }

    def querySpec() = {
      val spec = new QuerySpec()
        .withReturnConsumedCapacity(ReturnConsumedCapacity.INDEXES)

      val (aggregates, fields) = resolveProjection(query.projection)
      val withFields = if (fields.nonEmpty) {
        spec.withAttributesToGet(fields: _*)
      } else spec

      (aggregates, query.limit.fold(withFields) {
        limit => withFields.withMaxResultSize(limit)
      }
        .withMaxPageSize(pageSize)
        .withScanIndexForward(scanForward(query.orderBy.flatMap(_.direction))))
    }

    def querySingle(hash: Key, range: Key) = {
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
            throw new Exception(s"Schema '${schema.name}' does not contain a range key")
        }
      }

      // Because indexes can have duplicates, GetItemSpec cannot be used. Query must be used instead.
      def queryIndex(index: Index) = {
        validateKeys(index)

        val (aggregates, spec) = querySpec()
        val results = wrap(table.getIndex(index.name).query(
          spec
            .withHashKey(hashKey, unwrap(hashValue))
            .withRangeKeyCondition(
              new RangeKeyCondition(rangeKey).eq(unwrap(rangeValue))
            )
        //TODO: support order by on index queries?
        //.withScanIndexForward()
        ))
        (aggregates, results)
      }

      def getItem() = {
        validateKeys(tableDescription)

        val spec = new GetItemSpec()

        val withKey = spec.withPrimaryKey(
          hashKey,
          unwrap(hashValue),
          rangeKey,
          unwrap(rangeValue)
        )

        val (aggregates, fields) = resolveProjection(query.projection)
        val withFields = if (fields.nonEmpty) {
          withKey.withAttributesToGet(fields: _*)
        } else spec

        val results = ResultSet(Iterator(
          Timed(Try(Option(table.getItem(withFields)).map(Eval.jsonNode).toList))
        ))
        (aggregates, results)
      }

      //TODO: check types as well?
      query.useIndex
        .map(validateIndex(tableDescription, _, hash.field, Some(range.field)))
        .orElse(assumeIndex(
          tableCache,
          tableName = query.from,
          hashKey = hashKey,
          rangeKey = Some(rangeKey),
          orderBy = query.orderBy
        ))
        .map { index =>
          validateOrderBy(index.hash, index.range, query.orderBy)
          index
        }
        .map(index => queryIndex(index))
        .getOrElse(getItem())
    }

    def range(field: String, value: KeyValue) = {
      val (aggregates, spec) = querySpec()

      val results = query.useIndex
        .map(validateIndex(tableDescription, _, field, None))
        .orElse {
          def sortByTableField = query.orderBy.forall { order =>
            order.field == tableDescription.hash.name || tableDescription.range.exists(_.name == field)
          }
          // If the main table has the correct hash key, use that. Otherwise, check indexes. This is to avoid the
          // addition of a single local secondary index forcing all queries to add a 'use index' clause.
          if (tableDescription.hash.name == field && sortByTableField) {
            None
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
        .map { index =>
          validateOrderBy(index.hash, index.range, query.orderBy)
          table.getIndex(index.name)
            .query(spec.withHashKey(field, unwrap(value)))
        }
        .getOrElse {
          validateOrderBy(tableDescription.hash, tableDescription.range, query.orderBy)
          table.query(spec.withHashKey(field, unwrap(value)))
        }
      (aggregates, wrap(results))
    }

    def scan() = {
      //TODO: scan doesn't support order (because doesn't on hash key?)
      val spec = new ScanSpec()
      val (aggregates, fields) = resolveProjection(query.projection)
      val withFields = if (fields.nonEmpty) {
        spec.withAttributesToGet(fields: _*)
      } else spec

      val withLimit = query.limit.fold(withFields) {
        limit => withFields.withMaxResultSize(limit)
      }
        .withMaxPageSize(pageSize)
      val results = table.scan(withLimit)
      (aggregates, wrap(results))
    }

    //TODO: abstract over GetItemSpec, QuerySpec and ScanSpec?
    val (aggregates, results) = query.where match {
      case Some(Ast.PrimaryKey(hash, Some(range))) =>
        querySingle(hash = hash, range = range)
      case Some(Ast.PrimaryKey(Ast.Key(field, value), None)) =>
        range(field, value)
      case None =>
        if (query.orderBy.isDefined) {
          throw new Exception("'order by' is not currently supported on scan queries")
        }
        scan()
    }

    applyAggregates(aggregates, results)
  }
}
