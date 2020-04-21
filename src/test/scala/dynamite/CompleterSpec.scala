package dynamite

import zio.test._
import zio.test.Assertion._
import zio.{Ref, Task}
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import dynamite.Response.{KeySchema, TableDescription}

object CompleterSpec extends DefaultRunnableSpec {
  def spec = suite("completer")(
    test("parse a projection") {
      assert(
        fastparse
          .parse("select * from tableName", Completer.tableParser(_))
      ) {
        equalTo(
          fastparse.Parsed.Success("tableName", 23)
        )
      }
    },
    testM("cache table names") {
      val table = TableDescription(
        "playlists",
        KeySchema("id", ScalarAttributeType.S),
        None,
        Seq.empty
      )
      for {
        ref <- Ref.make((_: String) => Task.succeed(table))
        f2 = (_: String) => Task.fail(new Exception("fail")).orDie
        f1 <- ref.get
        loader = (input: String) => f1(input).tap(_ => ref.set(f2))
        cache = new TableCache(loader)
        value1 <- cache.get("playlists")
        value2 <- cache.get("playlists")
      } yield assert(value1)(isSome(equalTo(table))) &&
        assert(value2)(isSome(equalTo(table)))
    }
  )
}
