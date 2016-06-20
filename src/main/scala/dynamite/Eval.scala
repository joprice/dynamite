package dynamite

import dynamite.Ast.{ PrimaryKey => _, _ }
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.{ PrimaryKey => DynamoPrimaryKey, Index => _, _ }
import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.model.{ Select => _, TableDescription => _, _ }
import java.util.{ Iterator => JIterator }

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.{ Failure, Try }

object Eval {
  def apply[A](client: AmazonDynamoDB) = new Eval(client)
}

import Response._

case class AmbiguousIndexException(indexes: Seq[String]) extends Exception(
  s"""Multiple indexes can fulfill the query: ${indexes.mkString("[", ", ", "]")}.
      |Choose an index explicitly by using a 'use index' clause.""".stripMargin
)

class Eval(client: AmazonDynamoDB, pageSize: Int = 20) {
  val dynamo = new DynamoDB(client)

  def run(query: Query): Try[Response] = query match {
    case query: Select => select(query)
    case query: Update => update(query)
    case query: Delete => delete(query)
    case query: Insert => insert(query)
    case ShowTables => showTables()
    case DescribeTable(table) => describeTable(table)
  }

  def describeTable(tableName: String): Try[TableDescription] = Try {
    val description = table(tableName).describe()
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

  def showTables(): Try[TableNames] = Try {
    val it = dynamo.listTables().pages().iterator()
    TableNames(new RecoveringIterator(it).map {
      value => Timed(value.map(_.asScala.toList.map(_.getTableName)))
    })
  }

  def insert(query: Insert): Try[Complete.type] = Try {
    table(query.table).putItem(
      query.values.foldLeft(new Item()) {
        case (item, (field, value)) => item.`with`(field, unwrap(value))
      }
    )
    Complete
  }

  //TODO: condition expression that item must exist?
  def delete(query: Delete): Try[Complete.type] = Try {
    table(query.table).deleteItem(toDynamoKey(query.key))
    Complete
    //TODO: optionally return consumed capacity
    //.getConsumedCapacity
  }

  private def table(tableName: String) = dynamo.getTable(tableName)

  private def unwrap(value: Value): AnyRef = value match {
    case StringValue(value) => value
    case number: NumberValue => number.value
    case ListValue(value) => value.map(unwrap).asJava
  }

  def toDynamoKey(key: Ast.PrimaryKey) = {
    val Ast.PrimaryKey(hash, range) = key
    def toAttr(key: Ast.Key) = new KeyAttribute(key.field, unwrap(key.value))
    val attrs = toAttr(key.hash) +: range.map(toAttr).toSeq
    new DynamoPrimaryKey(attrs: _*)

  }

  //TODO: change response to more informative type
  def update(query: Update): Try[Complete.type] = {
    handleFailure(query.table, Try {
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

  private def handleFailure[A](table: String, result: Try[A]) = {
    result.recoverWith {
      case ex: ResourceNotFoundException =>
        Failure(new Exception(s"Table '$table' does not exist"))
    }
  }

  class TableIterator[A](
      select: Select,
      original: JIterator[Page[Item, A]]
  ) extends Iterator[Timed[Try[List[Item]]]] {
    private[this] val recovering = new RecoveringIterator(original)

    def next() = Timed {
      val result = recovering.next().map(_.asScala.toList)
      handleFailure(select.from, result)
    }

    def hasNext = recovering.hasNext
  }

  def select(query: Select): Try[ResultSet] = Try {
    def wrap[A](results: ItemCollection[A]) = {
      val original = results.pages().iterator()
      ResultSet(
        new TableIterator(query, original),
        Some(() => results.getAccumulatedConsumedCapacity)
      )
    }

    //TODO: handle pagination - use withMaxPageSize instead?
    def scanForward(dir: Option[Direction]) = dir match {
      case Some(Ascending) => true
      case Some(Descending) => false
      case None => true
    }

    //TODO: what to do when field does not exist on object?
    val table = dynamo.getTable(query.from)

    def querySpec() = {
      val spec = new QuerySpec()
        .withReturnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
      val withFields = query.projection match {
        case All => spec
        case Fields(fields) => spec.withAttributesToGet(fields: _*)
      }
      query.limit.fold(withFields) {
        limit => withFields.withMaxResultSize(limit)
      }
        .withMaxPageSize(pageSize)
        .withScanIndexForward(scanForward(query.direction))
    }

    //TODO: abstract over GetItemSpec, QuerySpec and ScanSpec?
    query.where match {
      case Some(Ast.PrimaryKey(Ast.Key(hashKey, hashValue), Some(Ast.Key(sortKey, sortValue)))) =>
        val grouped = describeTable(query.from).get.indexes.groupBy { index =>
          (index.hash.name, index.range.map(_.name))
        }.mapValues(_.map(_.name))

        //TODO: check types as well?
        query.useIndex.orElse(
          grouped.get(hashKey -> Some(sortKey)).map {
            case Seq(indexName) => indexName
            case indexes => throw new AmbiguousIndexException(indexes)
          }
        ).map { indexName =>
            val spec = querySpec()
              .withHashKey(hashKey, unwrap(hashValue))
              .withRangeKeyCondition(
                new RangeKeyCondition(sortKey).eq(unwrap(sortValue))
              )
            wrap(table.getIndex(indexName).query(spec))
          }.getOrElse {
            val spec = new GetItemSpec()
            val withFields = (query.projection match {
              case All => spec
              case Fields(fields) => spec.withAttributesToGet(fields: _*)
            }).withPrimaryKey(
              hashKey,
              unwrap(hashValue),
              sortKey,
              unwrap(sortValue)
            )
            ResultSet(Iterator(
              Timed(Try(Option(table.getItem(withFields)).toList))
            ))
          }

      case Some(Ast.PrimaryKey(Ast.Key(key, value), None)) =>
        val spec = querySpec().withHashKey(key, unwrap(value))
        wrap(table.query(spec))

      case None =>
        //TODO: scan doesn't support order (because doesn't on hash key?)
        val spec = new ScanSpec()
        val withFields = query.projection match {
          case All => spec
          case Fields(fields) => spec.withAttributesToGet(fields: _*)
        }
        val withLimit = query.limit.fold(withFields) {
          limit => withFields.withMaxResultSize(limit)
        }
          .withMaxPageSize(pageSize)
        wrap(table.scan(withLimit))
    }
  }

}

