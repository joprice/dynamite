package dynamite

import dynamite.Ast.{PrimaryKey => _, _}
import com.amazonaws.services.dynamodbv2.model.{
  Delete => _,
  Select => _,
  TableDescription => _,
  Update => _,
  _
}
import dynamite.Ast.Projection.{Aggregate, FieldSelector}
import dynamite.Response._

import scala.jdk.CollectionConverters._
import dynamite.Dynamo.Dynamo
import zio._
import zio.clock.Clock
import zio.stream.ZStream

object Eval {

  type Env = Clock with Dynamo.Dynamo

  def apply(
      query: Query,
      tableCache: TableCache[Dynamo],
      pageSize: Int
  ): RIO[Dynamo.Dynamo, Response] =
    query match {
      case query: Select =>
        select(query, pageSize, tableCache)
      case query: Update      => update(query)
      case query: Delete      => delete(query)
      case query: Insert      => insert(query, tableCache)
      case query: CreateTable => createTable(query)
      case ShowTables         => Task.succeed(showTables)
      case DescribeTable(table) =>
        tableCache
          .get(table)
          .map(_.getOrElse(Response.Info(s"No table exists with name $table")))
    }

  def assumeIndex(
      tableCache: TableCache[Dynamo],
      tableName: String,
      hashKey: String,
      rangeKey: Option[String],
      orderBy: Option[OrderBy]
  ): RIO[Dynamo, Option[Index]] =
    for {
      table <- tableCache
        .get(tableName)
        .foldM(
          error => Task.fail(error),
          table =>
            table
              .map(Task.succeed(_))
              .getOrElse(Task.fail(UnknownTableException(tableName)))
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
  ): Task[Unit] =
    orderBy.fold[Task[Unit]](Task.unit) { order =>
      val validOrder =
        range.fold(order.field == hash.name)(_.name == order.field)
      if (validOrder)
        Task.unit
      else
        Task.fail(new Exception(s"'${order.field}' is not a valid sort field."))
    }

  def validateIndex(
      tableDescription: TableDescription,
      indexName: String,
      hashKey: String,
      rangeKey: Option[String]
  ): Task[Index] =
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

  //TODO: handle pagination - use withMaxPageSize instead?
  def scanForward(dir: Option[Direction]) = dir match {
    case Some(Ascending)  => true
    case Some(Descending) => false
    case None             => true
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
  ): ResultSet =
    // For now, using headOption, since the only aggregate supported at the moment is Count(*) and duplicate
    // aggregates will be resolved by reference
    aggregates.headOption.fold(resultSet) {
      case agg: Aggregate.Count.type =>
        resultSet.copy(
          results = {
            //TODO: restore duration sum
            ZStream.fromEffect(
              resultSet.results
                .map(_.result.data.size)
                .fold(0)(_ + _)
                .map { count =>
                  import scala.concurrent.duration._
                  Timed(
                    Page(
                      List(
                        Map(
                          agg.name -> toAttributeValue(
                            Ast.IntValue(count.toString)
                          )
                        )
                      ),
                      hasMore = false
                    ),
                    0.second
                  )
                }
            )
          }
        )
    }

  //TODO: use UnknownTableException
  def recoverQuery[R, A](table: String, result: RIO[Dynamo, A]) =
    result.mapError {
      case _: ResourceNotFoundException =>
        new Exception(s"Table '$table' does not exist")
      case other => other
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
        s"Range key of '${range.name}' of index '$indexName' does not match provided hash key '$rangeKey'."
      )

  def describeTable(tableName: String): RIO[Dynamo, TableDescription] =
    for {
      result <- Dynamo.describeTable(tableName)
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

  def showTables: TableNames = TableNames(
    Dynamo.listTables.mapM(value => Timed(Task.succeed(value)))
  )

  def toScalarAttribute(typeName: String) =
    typeName match {
      case "string" => ZIO.succeed(ScalarAttributeType.S)
      case other =>
        ZIO.fail(new IllegalArgumentException(s"Invalid scalar type $other"))
    }

  def createTable(table: CreateTable) =
    for {
      typeName <- toScalarAttribute(table.typeName)
      result <- Dynamo
        .createTable(
          tableName = table.tableName,
          hash = (table.name, typeName),
          range = None,
          ignoreExisting = table.ignoreExisting
        )
        .as(Complete)
    } yield result

  def loadTableDescription[R](tableCache: TableCache[R], tableName: String) =
    tableCache
      .get(tableName)
      .foldM(
        {
          case _: ResourceNotFoundException =>
            Task.fail(UnknownTableException(tableName))
          case error => Task.fail(error)
        },
        table =>
          table.fold[Task[TableDescription]](
            Task.fail(UnknownTableException(tableName))
          )(Task.succeed(_))
      )

  def insert[R](
      query: Insert,
      tableCache: TableCache[R]
  ): RIO[R with Dynamo, Complete.type] = {
    def toKey(name: String, value: Value): Task[Key] =
      value match {
        //TODO: case object exception
        case value: KeyValue => Task.succeed(Key(name, value))
        case _: BoolValue | _: ListValue | _: ObjectValue =>
          Task.fail(
            new Exception(
              s"Invalid type for key $name: ${Value.typeName(value)}. Keys may only be strings or numbers"
            )
          )
      }
    for {
      tableDescription <- loadTableDescription(tableCache, query.table)
      hash <- query.values
        .find(_._1 == tableDescription.hash.name)
        .map((toKey _).tupled)
        .getOrElse(
          Task.fail(
            new Exception(s"Missing hash key ${tableDescription.hash.name}")
          )
        )
      range <- ZIO.foreach(
        tableDescription.range
          .map(_.name)
          .flatMap(value => query.values.find(_._1 == value))
      )((toKey _).tupled)
      () <- validateKeyType(
        tableDescription,
        index = None,
        //TODO: make this not take option (only applies to scan)
        primaryKey = Ast.PrimaryKey(hash, range)
      )
      item = query.values
        .foldLeft(Map[String, AttributeValue]()) {
          case (item, (field, value)) =>
            item.updated(field, toAttributeValue(value))
        }
      //TODO: timed
      result <- Dynamo
        .putItem(query.table, item)
        .as(Complete)
    } yield result
  }

  //TODO: condition expression that item must exist?
  def delete(
      query: Delete
  ): RIO[Dynamo, Complete.type] = {
    //TODO: optionally return consumed capacity
    //.getConsumedCapacity
    val Key(hashKey, hashValue) = query.key.hash
    val range = query.key.range.map {
      case Key(rangeKey, rangeValue) => (rangeKey, toAttributeValue(rangeValue))
    }
    //TODO: timed
    Dynamo
      .deleteItem(
        tableName = query.table,
        hash = (hashKey, toAttributeValue(hashValue)),
        range = range
      )
      .as(Complete)
  }

  //TODO: change response to more informative type
  def update(
      query: Update
  ): RIO[Dynamo, Complete.type] =
    recoverQuery(
      query.table, {
        val Key(hashKey, hashValue) = query.key.hash
        Dynamo
          .updateItem(
            query.table,
            hash = hashKey -> toAttributeValue(hashValue),
            range = query.key.range.map {
              case Key(rangeKey, rangeValue) =>
                rangeKey -> toAttributeValue(rangeValue)
            },
            fields = query.fields.map {
              case (key, value) => key -> toAttributeValue(value)
            }
          )
          .as(Complete)
      }
    )

  def validateKeyType(
      tableDescription: TableDescription,
      index: Option[Index],
      primaryKey: Ast.PrimaryKey
  ): Task[Unit] = {
    def validate(schema: KeySchema, key: KeySchema) = {
      def renderScalar(`type`: ScalarAttributeType) =
        `type` match {
          case ScalarAttributeType.S => "string"
          case ScalarAttributeType.N => "number"
          case ScalarAttributeType.B => "binary"
        }
      if (schema.`type` != key.`type`) {
        val expected = renderScalar(schema.`type`)
        val actual = renderScalar(key.`type`)
        Task
          .fail(
            new Exception(
              s"Wrong type for key ${key.name}. Expected $expected, found $actual"
            )
          )
      } else Task.unit
    }
    def keyToKeySchema(key: Key) =
      KeySchema(key.field, key.value match {
        case _: StringValue => ScalarAttributeType.S
        case _: NumberValue => ScalarAttributeType.N
      })

    val hash = keyToKeySchema(primaryKey.hash)
    val range = primaryKey.range.map(keyToKeySchema)
    val (tableHash, tableRange) = index
      .map(index => (index.hash, index.range))
      .getOrElse((tableDescription.hash, tableDescription.range))

    validate(tableHash, hash) *>
      tableRange
        .zip(range)
        .fold[Task[Unit]](Task.unit)((validate _).tupled)
  }

  def select(
      query: Select,
      pageSize: Int,
      tableCache: TableCache[Dynamo]
  ): RIO[Dynamo, ResultSet] = {
    //TODO stream of pages
    def wrap[A](
        data: RIO[
          Env,
          Timed[java.util.List[java.util.Map[String, AttributeValue]]]
        ],
        capacity: RIO[Env, Option[() => ConsumedCapacity]]
    ) =
      ResultSet(
        //TODO: recoverQuery
        ZStream.fromIterableM(
          data.map { data =>
            List(
              data.map { data =>
                Page(
                  data.asScala.map(_.asScala.toMap).toList,
                  hasMore = false
                )
              }
            )
          }
        ),
        capacity
        //TODO:
        //Some(() => results.get  AccumulatedConsumedCapacity)
      )

    def querySingle(
        tableDescription: TableDescription,
        hash: Key,
        range: Key
    ) = {
      val Key(hashKey, hashValue) = hash
      val Key(rangeKey, rangeValue) = range

      def validateKeys(schema: TableSchema): Task[Unit] =
        if (hashKey != schema.hash.name) {
          Task.fail(new Exception(s"Invalid hash key '$hashKey'"))
        } else {
          schema.range match {
            case Some(range) =>
              Task
                .fail(new Exception(s"Invalid range key '$rangeKey'"))
                .when(rangeKey != range.name)
            case None =>
              Task.fail(
                new Exception(
                  s"Schema '${schema.name}' does not contain a range key"
                )
              )
          }
        }

      // Because indexes can have duplicates, GetItemSpec cannot be used. Query must be used instead.
      def queryIndex(index: Index) =
        for {
          () <- validateKeys(index)
        } yield {
          val (aggregates, fields) = resolveProjection(query.projection)
          val results = Timed(
            Dynamo.query(
              tableName = query.from,
              hash = (hashKey, toAttributeValue(hashValue)),
              range = Some(rangeKey -> toAttributeValue(rangeValue)),
              index = Some(index.name),
              fields = fields,
              limit = query.limit,
              //TODO: support order by on index queries?
              scanForward = true
            )
          )
          (
            aggregates,
            wrap(
              results.map(_.map(_.getItems)),
              results.map(data => Some(() => data.result.getConsumedCapacity))
            )
          )
        }

      def getItem(tableDescription: TableDescription) = {
        val Key(hashKey, hashValue) = hash
        val Key(rangeKey, rangeValue) = range
        val (aggregates, fields) = resolveProjection(query.projection)
        validateKeys(tableDescription).as {
          val results = Timed(
            Dynamo.getItem(
              tableName = query.from,
              hash = hashKey -> toAttributeValue(hashValue),
              range = rangeKey -> toAttributeValue(rangeValue),
              fields = fields
            )
          )
          (
            aggregates,
            wrap(
              results.map(_.map(data => Option(data.getItem).toList.asJava)),
              Task(None)
            )
          )
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
        () <- ZIO.foreach_(query.where)(
          validateKeyType(tableDescription, index, _)
        )
        result <- index
          .map { index =>
            //TODO: check types as well?
            validateOrderBy(index.hash, index.range, query.orderBy) *> queryIndex(
              index
            )
          }
          .getOrElse(getItem(tableDescription))
      } yield result
    }

    def range(
        tableDescription: TableDescription,
        field: String,
        value: KeyValue
    ) = {
      val (aggregates, fields) = resolveProjection(query.projection)

      val results = query.useIndex
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
          ZIO.foreach_(query.where)(
            validateKeyType(tableDescription, maybeIndex, _)
          ) *>
            maybeIndex
              .map { index =>
                validateOrderBy(index.hash, index.range, query.orderBy) as
                  Timed(
                    Dynamo.query(
                      tableName = query.from,
                      hash = (field, toAttributeValue(value)),
                      range = None,
                      index = Some(index.name),
                      fields = fields,
                      limit = query.limit,
                      scanForward =
                        scanForward(query.orderBy.flatMap(_.direction))
                    )
                  )
              }
              .getOrElse {
                validateOrderBy(
                  tableDescription.hash,
                  tableDescription.range,
                  query.orderBy
                ) as
                  //TODO: combine with above (just return index)
                  Timed(
                    Dynamo.query(
                      tableName = query.from,
                      hash = (field, toAttributeValue(value)),
                      range = None,
                      index = None,
                      fields = fields,
                      limit = query.limit,
                      scanForward =
                        scanForward(query.orderBy.flatMap(_.direction))
                    )
                  )
              }
        }
      results.map { results =>
        (
          aggregates,
          wrap(
            results.map(_.map(_.getItems)),
            results.map(value => Some(() => value.result.getConsumedCapacity))
          )
        )
      }
    }

    def scan() = {
      //TODO: scan doesn't support order (because doesn't on hash key?)
      val (aggregates, fields) = resolveProjection(query.projection)
      val results =
        ZStream(
          Dynamo
            .scan(query.from, fields, limit = query.limit)
            .map(Page.tupled)
            .process
            //TODO: replace with built-in .timed
            .map(Timed.apply(_))
        )
      (
        aggregates,
        ResultSet(
          results,
          ZIO.none
          //TODO:
          //Some(() => results.get  AccumulatedConsumedCapacity)
        )
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
            )(Task.succeed(_))
        )
      //TODO: abstract over GetItemSpec, QuerySpec and ScanSpec?
      result <- query.where match {
        case Some(Ast.PrimaryKey(hash, Some(range))) =>
          querySingle(tableDescription, hash = hash, range = range)
        case Some(Ast.PrimaryKey(Ast.Key(field, value), None)) =>
          range(tableDescription, field, value)
        case None =>
          if (query.orderBy.isDefined) {
            Task.fail(
              new Exception(
                "'order by' is not currently supported on scan queries"
              )
            )
          } else {
            Task.succeed(scan())
          }
      }
    } yield {
      val (aggregates, results) = result
      applyAggregates(aggregates, results)
    }
  }
}
