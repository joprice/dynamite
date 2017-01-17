package dynamite

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import dynamite.Response.{ KeySchema, TableDescription }
import org.scalamock.scalatest.MockFactory
import org.scalatest._

import scala.util.{ Success, Try }

class CompleterSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with OptionValues {

  "table parser" should "parse a projection" in {
    Completer.tableParser.parse("select * from tableName") should matchPattern {
      case fastparse.core.Parsed.Success("tableName", _) =>
    }
  }

  "table cache" should "cache table names" in {
    val loader = mock[String => Try[TableDescription]]
    (loader.apply _)
      .expects("playlists")
      .returns(Success(TableDescription(
        "playlists",
        KeySchema("id", ScalarAttributeType.S),
        None,
        Seq.empty
      )))
      .noMoreThanOnce()

    val cache = new TableCache(loader)
    cache.get("playlists")
    cache.get("playlists")
  }

}

