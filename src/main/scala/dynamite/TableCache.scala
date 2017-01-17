package dynamite

import dynamite.Response.TableDescription

import scala.collection.concurrent.TrieMap
import scala.util.{ Success, Try }
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException

class TableCache(describeTable: String => Try[TableDescription]) {
  private[this] val tableNameCache = TrieMap[String, TableDescription]()

  def clear() = tableNameCache.clear()

  def get(tableName: String): Try[Option[TableDescription]] = {
    tableNameCache.get(tableName).map { value =>
      Success(Some(value))
    }.getOrElse {
      describeTable(tableName).map { result =>
        tableNameCache.put(tableName, result)
        Option(result)
      }.recoverWith {
        case _: ResourceNotFoundException => Success(None)
      }
    }
  }
}
