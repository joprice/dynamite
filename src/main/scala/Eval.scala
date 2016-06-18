package dynamite

import dynamite.Ast._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.{ PrimaryKey => DynamoPrimaryKey, _ }
import com.amazonaws.services.dynamodbv2.document.spec._
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import java.util.{ Iterator => JIterator }
import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

class RecoveringIterator[A, B](
    original: JIterator[Page[A, B]]
) extends Iterator[Try[JIterator[A]]] {
  private[this] var failed = false

  def next() = {
    val result = Try(original.next.iterator())
    failed = result.isFailure
    result
  }

  def hasNext = !failed && original.hasNext
}

object Eval {
  def apply[A](client: AmazonDynamoDB) = new Eval(client)
}

sealed abstract class Response
final case class ResultSet(results: Iterator[Try[List[Item]]]) extends Response
final case class TableNames(names: Iterator[Try[List[String]]]) extends Response
case object Complete extends Response

class Eval(client: AmazonDynamoDB, pageSize: Int = 5) {
  val dynamo = new DynamoDB(client)

  def run(query: Query): Try[Response] = query match {
    case select: Select => runSelect(select)
    case update: Update => runUpdate(update)
    case delete: Delete => runDelete(delete)
    case insert: Insert => runInsert(insert)
    case ShowTables => showTables()
  }

  def showTables(): Try[TableNames] = Try {
    val it = dynamo.listTables().pages().iterator()
    TableNames(new RecoveringIterator(it).map {
      _.map(_.asScala.toList.map(_.getTableName))
    })
  }

  def runInsert(insert: Insert): Try[Complete.type] = Try {
    table(insert.table).putItem(
      insert.values.foldLeft(new Item()) {
        case (item, (field, value)) => item.`with`(field, unwrap(value))
      }
    )
    Complete
  }

  //TODO: condition expression that item must exist?
  def runDelete(delete: Delete): Try[Complete.type] = Try {
    table(delete.table).deleteItem(toDynamoKey(delete.key))
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
  def runUpdate(update: Update): Try[Complete.type] = {
    handleFailure(update.table, Try {
      val table = dynamo.getTable(update.table)
      val Ast.PrimaryKey(hash, range) = update.key
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
        }.withAttributeUpdate(update.fields.map {
          case (key, value) => new AttributeUpdate(key).put(unwrap(value))
        }: _*)
      // TODO: id must exist condition
      table.updateItem(spec)
      Complete
    })
  }

  private def handleFailure[A](table: String, result: Try[A]) = {
    result.recoverWith {
      case _: ResourceNotFoundException =>
        Failure(new Exception(s"Table '$table' does not exist"))
    }
  }

  class TableIterator[A](
      select: Select,
      original: JIterator[Page[Item, A]]
  ) extends Iterator[Try[List[Item]]] {
    private[this] val recovering = new RecoveringIterator(original)

    def next() = {
      val result = recovering.next().map(_.asScala.toList)
      handleFailure(select.from, result)
    }

    def hasNext = recovering.hasNext
  }

  def runSelect(select: Select): Try[ResultSet] = Try {
    def render[A](results: ItemCollection[A]) = {
      val original = results.pages().iterator()
      new TableIterator(select, original)
    }

    //TODO: handle pagination - use withMaxPageSize instead?
    def scanForward(dir: Option[Direction]) = dir match {
      case Some(Ascending) => true
      case Some(Descending) => false
      case None => true
    }

    //TODO: what to do when field does not exist on object?
    val table = dynamo.getTable(select.from)

    //TODO: abstract over GetItemSpec, QuerySpec and ScanSpec?
    val result = select.where match {
      case Some(
        Ast.PrimaryKey(Ast.Key(hashKey, hashValue),
          Some(Ast.Key(sortKey, sortValue)))
        ) =>
        val spec = new GetItemSpec()
        val withFields = (select.projection match {
          case All => spec
          case Fields(fields) => spec.withAttributesToGet(fields: _*)
        }).withPrimaryKey(
          hashKey,
          unwrap(hashValue),
          sortKey,
          unwrap(sortValue)
        )
        val result = table.getItem(withFields)
        Iterator(Success(Option(result).toList))

      case Some(Ast.PrimaryKey(Ast.Key(key, value), None)) =>
        val spec = new QuerySpec()
        val withFields = select.projection match {
          case All => spec
          case Fields(fields) => spec.withAttributesToGet(fields: _*)
        }
        val withLimit = select.limit.fold(withFields) {
          limit => withFields.withMaxResultSize(limit)
        }
          .withHashKey(key, unwrap(value))
          .withMaxPageSize(pageSize)
          .withScanIndexForward(scanForward(select.direction))
        render(table.query(withLimit))

      case None =>
        //TODO: scan doesn't support order (because doesn't on hash key?)
        val spec = new ScanSpec()
        val withFields = select.projection match {
          case All => spec
          case Fields(fields) => spec.withAttributesToGet(fields: _*)
        }
        val withLimit = select.limit.fold(withFields) {
          limit => withFields.withMaxResultSize(limit)
        }
          .withMaxPageSize(pageSize)
        render(table.scan(withLimit))
    }

    ResultSet(result)
  }

}

