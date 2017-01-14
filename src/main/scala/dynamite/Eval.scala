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

  def apply(dynamo: DynamoDB, query: Query, pageSize: Int): Try[Response] = {
    query match {
      case query: Select => select(dynamo, query, pageSize)
      case query: Update => update(dynamo, query)
      case query: Delete => delete(dynamo, query)
      case query: Insert => insert(dynamo, query)
      case ShowTables => showTables(dynamo)
      case DescribeTable(table) => describeTable(dynamo, table)
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
    case StringValue(value) => value
    case number: NumberValue => number.value
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

      val spec = range.fold(
        baseSpec.withPrimaryKey(hash.field, unwrap(hash.value))
      ) { range =>
          baseSpec.withPrimaryKey(
            range.field,
            unwrap(range.value),
            hash.field,
            unwrap(hash.value)
          )
        }.withAttributeUpdate(query.fields.map {
          case (key, value) => new AttributeUpdate(key).put(unwrap(value))
        }: _*)
      // TODO: id must exist condition
      table.updateItem(spec)
      Complete
    })
  }

  def select(dynamo: DynamoDB, query: Select, pageSize: Int): Try[ResultSet] = Try {
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

    //TODO: what to do when field does not exist on object?
    val table = dynamo.getTable(query.from)

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
        .withScanIndexForward(scanForward(query.direction)))
    }

    def querySingle(hash: Key, range: Key) = {
      val Key(hashKey, hashValue) = hash
      val Key(rangeKey, rangeValue) = range

      def assumeIndex = {
        //TODO: cache this
        val grouped = describeTable(dynamo, query.from).getOrElse {
          throw UnknownTableException(query.from)
        }.indexes.groupBy { index =>
          (index.hash.name, index.range.map(_.name))
        }.mapValues(_.map(_.name))
        grouped.get(hashKey -> Some(rangeKey)).map {
          case Seq(indexName) => indexName
          case indexes => throw AmbiguousIndexException(indexes)
        }
      }

      // Because indexes can have duplicates, GetItemSpec cannot be used. Query must be used instead.
      def queryIndex(indexName: String) = {
        val (aggregates, spec) = querySpec()
        val results = wrap(table.getIndex(indexName).query(
          spec
            .withHashKey(hashKey, unwrap(hashValue))
            .withRangeKeyCondition(
              new RangeKeyCondition(rangeKey).eq(unwrap(rangeValue))
            )
        ))
        (aggregates, results)
      }

      def getItem() = {
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
      query.useIndex.orElse(assumeIndex)
        .map(queryIndex)
        .getOrElse(getItem())
    }

    def range(field: String, value: KeyValue) = {
      val (aggregates, spec) = querySpec()
      val results = table.query(spec.withHashKey(field, unwrap(value)))
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
      case Some(Ast.PrimaryKey(hash, Some(range))) => querySingle(hash = hash, range = range)
      case Some(Ast.PrimaryKey(Ast.Key(field, value), None)) => range(field, value)
      case None => scan()
    }

    applyAggregates(aggregates, results)
  }
}

