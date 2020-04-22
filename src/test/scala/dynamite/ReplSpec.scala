package dynamite

import zio.test._
import zio.test.Assertion._
import java.io._

import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  ScalarAttributeType
}
import dynamite.Ast.{DescribeTable, ShowTables}
import dynamite.Ast.Projection.FieldSelector.{All, Field}
import dynamite.Response.{Index, KeySchema}
import fansi.{Bold, Str}
import jline.internal.Ansi
import zio.{Task, ZIO, ZManaged}
import zio.logging.Logging
import zio.stream.ZStream
import zio.test.environment.TestClock

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object ReplSpec extends DefaultRunnableSpec {

  def withReader[A](query: String) =
    ZManaged.makeEffect(
      new dynamite.Reader.TestReader(query)
    )(_.close())

  def spec =
    suite("repl")(
      testM("tolerate failed responses") {
        val query = s"select * from playlists limit 1;"
        val writer = new StringWriter()
        withReader(query)
          .use { reader =>
            Repl.loop(
              "",
              new PrintWriter(writer),
              reader,
              Ast.Format.Tabular,
              _ => Task.fail(new Exception("fail"))
            )
          }
          .as(
            assert(writer.toString)(
              equalTo(
                s"""${Repl.formatError("fail")}
          |""".stripMargin
              )
            )
          )
      },
      testM("handle multiline queries") {
        val query =
          """select * from playlists
          |
          |
          |limit 1;
        """.stripMargin
        val writer = new StringWriter()
        withReader(query)
          .use { reader =>
            Repl.loop(
              "",
              new PrintWriter(writer),
              reader,
              Ast.Format.Tabular,
              _ => Task.fail(new Exception("fail"))
            )
          }
          .as(
            assert(writer.toString)(
              equalTo(
                s"""${Repl.formatError("fail")}
           |""".stripMargin
              )
            )
          )
      },
      testM("paginate results") {
        val writer = new StringWriter()
        val query = s"select * from playlists where userId = 'user-id-1';"
        withReader(query)
          .use {
            reader =>
              Repl.loop(
                "",
                new PrintWriter(writer),
                reader,
                Ast.Format.Tabular,
                _ =>
                  Task.succeed(
                    Response.ResultSet(
                      ZStream(
                        Timed(
                          List {
                            Map(
                              "id" -> new AttributeValue().withS("abc"),
                              "size" -> new AttributeValue().withN("2")
                            )
                          },
                          1.second
                        ),
                        Timed(List {
                          Map(
                            "id" -> new AttributeValue().withS("def"),
                            "size" -> new AttributeValue().withN("2")
                          )
                        }, 1.second)
                      )
                    )
                  )
              )
          }
          .as(
            assert(writer.toString)(
              equalTo(
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
            )
          )
      },
      //TODO: at least print 0 rows returned?
      testM("render an empty result") {
        val query = "select * from playlists where userId = 'user-id-1';"
        val writer = new StringWriter()
        withReader(query)
          .use { reader =>
            Repl.loop(
              "",
              new PrintWriter(writer),
              reader,
              Ast.Format.Tabular, {
                case _: Ast.Select =>
                  Task.succeed(
                    Response.ResultSet(
                      ZStream.empty
                    )
                  )
                case _ => ???
              }
            )
          }
          .as(
            assert(Ansi.stripAnsi(writer.toString))(equalTo(""))
          )
      },
      testM("handle failed pagination") {
        val writer = new StringWriter()
        val query = s"select * from playlists where userId = 'user-id-1';"
        withReader(query)
          .use {
            reader =>
              Repl.loop(
                "",
                new PrintWriter(writer),
                reader,
                Ast.Format.Tabular,
                _ =>
                  Task.succeed(
                    Response.ResultSet(
                      ZStream.fromEffect(
                        ZIO
                          .succeed(Timed(List {
                            Map(
                              "id" -> new AttributeValue().withS("abc"),
                              "size" -> new AttributeValue().withN("2")
                            )
                          }, 1.second))
                      ) ++ ZStream.fromEffect(
                        Task.fail(new Exception("second page failed"))
                      )
                    )
                  )
              )
          }
          .as(
            assert(Ansi.stripAnsi(writer.toString))(
              //TODO: restore press any key output here (handle "hasNext" in ZStream)
              equalTo(
                s"""id      size
           |"abc"   2
           |
           |Completed in 1000 ms
           |[error] second page failed
           |""".stripMargin
              )
            )
          )
      },
      testM("render results in json") {
        val writer = new StringWriter()
        val query =
          s"""format json;
            select * from playlists where userId = 'user-id-1';""".stripMargin

        val format = Ast.Format.Tabular

        withReader(query)
          .use { reader =>
            Repl.loop(
              "",
              new PrintWriter(writer),
              reader,
              format, { _ =>
                Task.succeed(
                  Response.ResultSet(
                    ZStream(
                      Timed(List {
                        Map(
                          "id" -> new AttributeValue().withS("abc"),
                          "size" -> new AttributeValue().withN("2")
                        )
                      }, 1.second)
                    )
                  )
                )
              }
            )
          }
          .as(
            assert(writer.toString)(
              equalTo(
                """Format set to json
          |{
          |  "id" : "abc",
          |  "size" : 2
          |}
          |Completed in 1000 ms
          |""".stripMargin
              )
            )
          )
      },
      testM("support aggregates") {
        val writer = new StringWriter()
        val query = s"""format tabular;
                      select count(*) from playlists where userId = 'user-id-1' ;
      """
        withReader(query)
          .use { reader =>
            Repl.loop(
              "",
              new PrintWriter(writer),
              reader,
              Ast.Format.Tabular,
              _ =>
                Task.succeed(
                  Response.ResultSet(
                    ZStream(
                      Timed(
                        List {
                          Map(
                            "count" -> new AttributeValue().withN("20")
                          )
                        },
                        1.second
                      )
                    )
                  )
                )
            )
          }
          .as {
            assert(writer.toString)(
              equalTo(
                s"""Format set to tabular
          |${Bold.On(Str("count"))}
          |20
          |
          |Completed in 1000 ms
          |""".stripMargin
              )
            )
          }
      },
      test("get unique fields from a field projection") {
        assert(
          Repl.headers(
            Seq(
              Field("id"),
              Field("name")
            ),
            Seq()
          )
        )(equalTo(Seq("id", "name")))
      },
      test("get unique fields from an all projection") {
        assert(
          Repl.headers(
            Seq(
              All
            ),
            Seq(
              Map(
                "id" -> new AttributeValue().withN("1"),
                "name" -> new AttributeValue().withS("test")
              )
            )
          )
        )(equalTo(Seq("id", "name")))
      },
      test("call close") {
        var closed = false
        val closeable = new Closeable {
          def close() = closed = true
        }
        Repl.withCloseable(closeable)(_ => ())
        assert(closed)(isTrue)
      },
      test("truncate string over 40 chars") {
        assert(Repl.truncate("a" * 50).length)(equalTo(40))
      },
      test("not truncate when less than 40") {
        assert(Repl.truncate("a" * 30).length)(equalTo(30))
      },
      test("render unaggregated fields error") {
        val query = "select count(*), id from playlist"
        val error = Parser.parse(query).left.map { failure =>
          Repl.parseError("select count(*), id from playlist", failure)
        }
        assert(error)(
          isLeft(
            equalTo(
              Repl.formatError(
                "Aggregates may not be used with unaggregated fields"
              )
            )
          )
        )
      },
      test("render parse error") {
        val query = "select count(*) fron playlist"
        val error = Parser.parse(query).left.map { failure =>
          Repl.parseError(query, failure)
        }
        val result = s"""[error] Failed to parse query
         |select count(*) fron playlist
         |                ^""".stripMargin
        assert(error.left.map(Ansi.stripAnsi))(isLeft(equalTo(result)))
      },
      test("only show line where error occurred") {
        val query = """
          |select count(*)
          |
          |fron playlist""".stripMargin
        val error = Parser.parse(query).left.map { failure =>
          Ansi.stripAnsi(Repl.parseError(query, failure))
        }
        val result = s"""[error] Failed to parse query
                      |fron playlist
                      |^""".stripMargin
        assert(error)(isLeft(equalTo(result)))
      },
      testM("printTableDescription render a single table schema") {
        val query = "describe table playlists;"
        val writer = new StringWriter()
        withReader(query)
          .use {
            reader =>
              Repl.loop(
                "",
                new PrintWriter(writer),
                reader,
                Ast.Format.Tabular, {
                  case DescribeTable("playlists") =>
                    Task.succeed(
                      Response.TableDescription(
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
                              "created_date",
                              ScalarAttributeType.N
                            ),
                            Some(
                              KeySchema("id", ScalarAttributeType.S)
                            )
                          )
                        )
                      )
                    )
                  case _ => ???
                }
              )
          }
          .as(
            assert(Ansi.stripAnsi(writer.toString))(
              equalTo(
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
              )
            )
          )
      },
      testM("render a schema with range key") {
        val query = "describe table playlists;"
        val writer = new StringWriter()
        withReader(query)
          .use {
            reader =>
              Repl.loop(
                "",
                new PrintWriter(writer),
                reader,
                Ast.Format.Tabular, {
                  case DescribeTable("playlists") =>
                    Task.succeed(
                      Response.TableDescription(
                        "playlists",
                        KeySchema("userId", ScalarAttributeType.S),
                        Some(KeySchema("id", ScalarAttributeType.S)),
                        Seq.empty
                      )
                    )
                  case _ => ???
                }
              )
          }
          .as(
            assert(Ansi.stripAnsi(writer.toString))(
              equalTo(
                s"""schema
           |name        hash              range
           |playlists   userId [string]   id [string]
           |
           |indexes
           |name   hash   range
           |
           |""".stripMargin
              )
            )
          )
      },
      testM("show tables should render a list of tables") {
        val query = "show tables;"
        withReader(query)
          .use { reader =>
            val writer = new StringWriter()
            Repl
              .loop(
                "",
                new PrintWriter(writer),
                reader,
                Ast.Format.Tabular, {
                  case ShowTables =>
                    Task.succeed(
                      Response.TableNames(
                        ZStream(
                          Timed(List("playlists"), 1.second)
                        )
                      )
                    )
                  case _ => ???
                }
              )
              .as(writer.toString)
          }
          .map(out =>
            assert(Ansi.stripAnsi(out))(
              equalTo(
                s"""name
           |playlists
           |
           |Completed in 1000 ms
           |""".stripMargin
              )
            )
          )
      },
      test("render as row")(
        assert(
          Repl.renderRow(
            Map(
              "name" -> new AttributeValue().withS("name"),
              "id" -> new AttributeValue().withN("1"),
              "tracks" -> new AttributeValue().withL(
                new AttributeValue().withN("1"),
                new AttributeValue().withN("2"),
                new AttributeValue().withN("3")
              ),
              "ids" -> new AttributeValue().withNS(
                List("1", "2", "3").asJava
              ),
              "names" -> new AttributeValue().withSS(
                List("a", "b", "c").asJava
              ),
              "meta" -> new AttributeValue().withM(
                Map(
                  "created" -> new AttributeValue().withN("1"),
                  "updated" -> new AttributeValue().withN("2")
                ).asJava
              ),
              "missing" -> new AttributeValue().withNULL(true)
            )
          )
        )(
          equalTo(
            Map(
              "name" -> "\"name\"",
              "id" -> "1",
              "ids" -> "[1,2,3]",
              "names" -> "[\"a\",\"b\",\"c\"]",
              "tracks" -> "[1,2,3]",
              "meta" -> """{"created": 1, "updated": 2}""",
              "missing" -> "null"
            )
          )
        )
      )
    ).provideSomeLayer(Logging.ignore ++ TestClock.default) @@ TestAspect.sequential
}
