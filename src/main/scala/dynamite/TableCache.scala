package dynamite

import dynamite.Response.TableDescription
import scala.collection.concurrent.TrieMap
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import zio.Task

class TableCache(describeTable: String => Task[TableDescription]) {
  private[this] val tableNameCache = TrieMap[String, TableDescription]()

  def clear() = tableNameCache.clear()

  def get(tableName: String): Task[Option[TableDescription]] =
    tableNameCache
      .get(tableName)
      .map(value => Task.succeed(Some(value)))
      .getOrElse {
        describeTable(tableName)
          .map { result =>
            tableNameCache.put(tableName, result)
            Option(result)
          }
          .catchSome {
            case _: ResourceNotFoundException => Task.succeed(None)
          }
      }
}
