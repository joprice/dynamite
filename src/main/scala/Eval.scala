package dynamite

import dynamite.Ast._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{ GetItemSpec, QuerySpec, ScanSpec, UpdateItemSpec }
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import java.util.{ Iterator => JIterator }

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

object Eval {
  def apply[A](client: AmazonDynamoDB) = new Eval(client)
}

sealed abstract class Response
final case class ResultSet(results: Iterator[Try[List[String]]]) extends Response
case object Complete extends Response

class Eval(client: AmazonDynamoDB, defaultLimit: Int = 20) {
  val dynamo = new DynamoDB(client)

  def run(query: Query): Try[Response] = query match {
    case select: Select => runSelect(select)
    case update: Update => runUpdate(update)
  }

  private def unwrap(value: Value) = value match {
    case StringValue(value) => value
    case IntValue(value) => value
  }

  //TODO: change response to more informative type
  def runUpdate(update: Update): Try[Complete.type] = {
    handle(update.table, Try {
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

  private def handle[A](table: String, result: Try[A]) = {
    result.recoverWith {
      case _: ResourceNotFoundException =>
        Failure(new Exception(s"Table '$table' does not exist"))
    }
  }

  class RecoveringIterator[A](
      select: Select,
      original: JIterator[Page[Item, A]]
  ) extends Iterator[Try[List[String]]] {

    private[this] var failed = false

    def next() = {
      val result = Try(
        original.next.iterator().asScala.toList.map(_.toJSON)
      )
      failed = result.isFailure
      handle(select.from, result)
    }

    def hasNext = !failed && original.hasNext
  }

  def runSelect(select: Select): Try[ResultSet] = Try {
    def render[A](results: ItemCollection[A]) = {
      val original = results.pages().iterator()
      new RecoveringIterator(select, original)
    }

    //TODO: handle pagination - use withMaxPageSize instead?
    def scanForward(dir: Option[Direction]) = dir match {
      case Some(Ascending) => true
      case Some(Descending) => false
      case None => true
    }

    //TODO: what to do when field does not exist on object?
    val table = dynamo.getTable(select.from)
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
        Iterator(Success(List(result.toJSON)))

      case Some(Ast.PrimaryKey(Ast.Key(key, value), None)) =>
        val spec = new QuerySpec()
        val withFields = (select.projection match {
          case All => spec
          case Fields(fields) => spec.withAttributesToGet(fields: _*)
        })
          .withHashKey(key, unwrap(value))
          .withMaxPageSize(select.limit.getOrElse(defaultLimit))
          //true - asc, false - decs
          .withScanIndexForward(scanForward(select.direction))
        render(table.query(withFields))

      case None =>
        //TODO: scan doesn't support order (because doesn't on hash key?)
        val spec = new ScanSpec()
        val withFields = (select.projection match {
          case All => spec
          case Fields(fields) => spec.withAttributesToGet(fields: _*)
        })
          .withMaxPageSize(select.limit.getOrElse(defaultLimit))
        render(table.scan(withFields))
    }

    ResultSet(result)
  }

}

