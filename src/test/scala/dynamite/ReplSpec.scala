package dynamite

import java.io._

import com.amazonaws.jmespath.ObjectMapperSingleton
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.util.json.Jackson
import com.fasterxml.jackson.databind.JsonNode
import dynamite.Ast.DescribeTable
import dynamite.Ast.Projection.FieldSelector.{ All, Field }
import dynamite.Response.{ Index, KeySchema }
import fansi.{ Bold, Color, Str }
import org.scalatest._
import jline.internal.Ansi

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }
import org.scalamock.scalatest.MockFactory

class ReplSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {

  def withReader[A](query: String)(f: Reader => A) = {
    val reader = new TestReader(query)
    try f(reader) finally {
      reader.close()
    }
  }

  "repl" should "tolerate failed responses" in {
    val query = s"select * from playlists limit 1;"
    val writer = new StringWriter()
    withReader(query) { reader =>
      Repl.loop(
        "",
        new PrintWriter(writer),
        reader,
        Ast.Format.Tabular,
        _ => Failure(new Exception("fail"))
      )
    }
    writer.toString should be(
      s"""${Repl.formatError("fail")}
        |""".stripMargin
    )
  }

  it should "handle multiline queries" in {
    val query =
      """select * from playlists
        |
        |
        |limit 1;
      """.stripMargin
    val writer = new StringWriter()
    withReader(query) { reader =>
      Repl.loop(
        "",
        new PrintWriter(writer),
        reader,
        Ast.Format.Tabular,
        _ => Failure(new Exception("fail"))
      )
    }
    writer.toString should be(
      s"""${Repl.formatError("fail")}
         |""".stripMargin
    )
  }

  it should "paginate results" in {
    val writer = new StringWriter()
    val query = s"select * from playlists where userId = 'user-id-1';"
    withReader(query) { reader =>
      Repl.loop(
        "",
        new PrintWriter(writer),
        reader,
        Ast.Format.Tabular,
        _ => Try(
          Response.ResultSet(
            Iterator(
              Timed(Success(List {
                val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
                node.put("id", "abc")
                node.put("size", 2)
              }), 1.second),
              Timed(Success(List {
                val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
                node.put("id", "def")
                node.put("size", 2)
              }), 1.second)
            )
          )
        )
      )
    }
    writer.toString should be(
      s"""${Bold.On("id")}      ${Bold.On("size")}
         |"abc"   2
         |
         |Completed in 1000 ms
         |Press q to quit, any key to load next page.
         |${Bold.On("id")}      ${Bold.On("size")}
         |"def"   2
         |
         |Completed in 1000 ms
         |""".stripMargin
    )
  }

  it should "handle failing pagination" in {
    val writer = new StringWriter()
    val query = s"select * from playlists where userId = 'user-id-1';"
    withReader(query) { reader =>
      Repl.loop(
        "",
        new PrintWriter(writer),
        reader,
        Ast.Format.Tabular,
        _ => Try(
          Response.ResultSet(
            Iterator(
              Timed(Success(List {
                val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
                node.put("id", "abc")
                node.put("size", 2)
              }), 1.second),
              Timed(Failure(new Exception("second page failed")))
            )
          )
        )
      )
    }
    Ansi.stripAnsi(writer.toString) should be(
      s"""id      size
         |"abc"   2
         |
         |Completed in 1000 ms
         |Press q to quit, any key to load next page.
         |[error] second page failed
         |""".stripMargin
    )
  }

  it should "render results in json" in {
    val writer = new StringWriter()
    val query =
      s"""format json;
          select * from playlists where userId = 'user-id-1';""".stripMargin

    val format = Ast.Format.Tabular

    withReader(query) { reader =>
      Repl.loop(
        "",
        new PrintWriter(writer),
        reader,
        format,
        {
          _ =>
            Try(
              Response.ResultSet(
                Iterator(
                  Timed(Success(List {
                    val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
                    node.put("id", "abc")
                    node.put("size", 2)
                  }), 1.second)
                )
              )
            )
        }
      )
    }
    writer.toString should be(
      """Format set to json
        |{
        |  "id": "abc",
        |  "size": 2
        |}
        |Completed in 1000 ms
        |""".stripMargin
    )
  }

  it should "support aggregates" in {
    val writer = new StringWriter()
    val query = s"select count(*) from playlists where userId = 'user-id-1' ;"
    withReader(query) { reader =>
      Repl.loop(
        "",
        new PrintWriter(writer),
        reader,
        Ast.Format.Tabular,
        _ => Try(
          Response.ResultSet(
            Iterator.single(Timed(Success(List {
              val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
              node.put("count", 20)
            }), 1.second))
          )
        )
      )
    }
    writer.toString should be(
      s"""${Bold.On(Str("count"))}
        |20
        |
        |Completed in 1000 ms
        |""".stripMargin
    )
  }

  "headers" should "get unique fields from a field projection" in {
    Repl.headers(Seq(
      Field("id"), Field("name")
    ), Seq( //Jackson.fromJsonString("""{"id": 1, "name": "test"}""", classOf[JsonNode])
    )) should be(Seq("id", "name"))
  }

  it should "get unique fields from an all projection" in {
    Repl.headers(Seq(
      All
    ), Seq(
      Jackson.fromJsonString("""{"id": 1, "name": "test"}""", classOf[JsonNode])
    )) should be(Seq("id", "name"))
  }

  "withCloseable" should "call close" in {
    var closed = false
    val closeable = new Closeable {
      def close() = closed = true
    }
    Repl.withCloseable(closeable)(_ => ())
    closed shouldBe true
  }

  "truncate" should "truncate string over 40 chars" in {
    Repl.truncate("a" * 50) should have length 40
  }

  it should "not truncate when less than 40" in {
    Repl.truncate("a" * 30) should have length 30
  }

  "parseError" should "render unaggregated fields error" in {
    val query = "select count(*), id from playlist"
    val failure = Parser(query).left.value
    val error = Repl.parseError("select count(*), id from playlist", failure)
    error shouldBe Repl.formatError("Aggregates may not be used with unaggregated fields")
  }

  it should "render parse error" in {
    val query = "select count(*) fron playlist"
    val failure = Parser(query).left.value
    val error = Repl.parseError(query, failure)
    val result = s"""[error] Failed to parse query
       |select count(*) fron playlist
       |                ^""".stripMargin
    Ansi.stripAnsi(error) shouldBe result
  }

  "errorLine" should "only show line where error occurred" in {
    val query = """
        |select count(*)
        |
        |fron playlist""".stripMargin
    val failure = Parser(query).left.value
    val error = Repl.parseError(query, failure)
    val result = s"""[error] Failed to parse query
                    |fron playlist
                    |^""".stripMargin
    Ansi.stripAnsi(error) shouldBe result
  }

  "printTableDescription" should "print a list of table schemas" in {
    val query = "describe table playlists;"
    val writer = new StringWriter()
    withReader(query) { reader =>
      Repl.loop(
        "",
        new PrintWriter(writer),
        reader,
        Ast.Format.Tabular, {
          case DescribeTable("playlists") =>
            Success(Response.TableDescription(
              "playlists",
              KeySchema("id", ScalarAttributeType.S),
              None,
              Seq(
                Index(
                  "by_image",
                  KeySchema("image", ScalarAttributeType.B),
                  None
                ),
                Index(
                  "by_created_date",
                  KeySchema(
                    "created_date", ScalarAttributeType.N
                  ),
                  Some(
                    KeySchema("id", ScalarAttributeType.S)
                  )
                )
              )
            ))
          case _ => ???
        }
      )
    }
    Ansi.stripAnsi(writer.toString) shouldBe
      s"""schema
        |name        hash          range
        |playlists   id [string]
        |
        |indexes
        |name              hash                    range
        |by_image          image [binary]${"       "}
        |by_created_date   created_date [number]   id [string]
        |
        |""".stripMargin
  }

}
