package dynamite

import java.io._
import java.util.concurrent.atomic.AtomicReference

import com.amazonaws.jmespath.ObjectMapperSingleton
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.util.json.Jackson
import com.fasterxml.jackson.databind.JsonNode
import dynamite.Ast.Projection.FieldSelector.{ All, Field }
import dynamite.Parser.ParseException.{ ParseError, UnAggregatedFieldsError }
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
      val repl = new Repl(
        _ => Failure(new Exception("fail")),
        reader,
        new PrintWriter(writer),
        new AtomicReference(Ast.Format.Tabular)
      )
      repl.run()
    }
    writer.toString should be(
      s"""${Repl.formatError("fail")}
        |""".stripMargin
    )
  }

  it should "support aggregates" in {
    val writer = new StringWriter()
    val query = s"select count(*) from playlists where userId = 'user-id-1' ;"
    withReader(query) { reader =>
      val repl = new Repl(
        _ => Try(
          Response.ResultSet(
            Iterator.single(Timed(Success(List {
              val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
              node.put("count", 20)
            }), 1.second))
          )
        ),
        reader,
        new PrintWriter(writer),
        new AtomicReference(Ast.Format.Tabular)
      ).run()
    }
    writer.toString should be(
      s"""${Bold.On(Str("count"))}
        |20 ${"  "}
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

}
