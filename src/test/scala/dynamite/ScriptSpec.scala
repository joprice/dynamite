package dynamite

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import dynamite.Dynamo.DynamoClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{
  AttributeDefinition,
  AttributeValue,
  KeySchemaElement,
  KeyType,
  ProvisionedThroughput,
  ScalarAttributeType
}
import zio.test.environment.TestConsole
import zio.{Task, ZManaged}
import zio.test._
import zio.test.Assertion._
import scala.jdk.CollectionConverters._
import DynamoDBTestHelpers._

object ScriptSpec extends DefaultRunnableSpec {
  private def attributeDefinitions(
      attributes: Seq[(String, ScalarAttributeType)]
  ) =
    attributes.map {
      case (symbol, attributeType) =>
        new AttributeDefinition(symbol, attributeType)
    }.asJava

  def createTable(
      client: AmazonDynamoDB
  )(tableName: String)(attributes: (String, ScalarAttributeType)*) =
    for {
      key <- keySchema(attributes)
      result <- Task(
        client.createTable(
          attributeDefinitions(attributes),
          tableName,
          key,
          new ProvisionedThroughput(1L, 1L)
        )
      )
    } yield result

  private def keySchema(attributes: Seq[(String, ScalarAttributeType)]) =
    attributes.toList match {
      case Nil => Task.fail(new Exception("Invalid key schema"))
      case hashKeyWithType :: rangeKeyWithType =>
        val keySchemas =
          hashKeyWithType._1 -> KeyType.HASH :: rangeKeyWithType.map(
            _._1 -> KeyType.RANGE
          )
        Task.succeed(
          keySchemas.map {
            case (symbol, keyType) => new KeySchemaElement(symbol, keyType)
          }.asJava
        )
    }

  def captureStdOut[A](f: => A): (String, A) = {
    val os = new ByteArrayOutputStream()
    val result = Console.withOut(os)(f)
    val output = new String(os.toByteArray, StandardCharsets.UTF_8)
    (output, result)
  }

  val insertRows =
    for {
      _ <- Dynamo.putItem(
        "playlists",
        Map(
          "id" -> new AttributeValue("123")
        )
      )
      _ <- Dynamo.putItem(
        "playlists",
        Map(
          "id" -> new AttributeValue("456")
        )
      )
    } yield ()

  val withPlaylistTable =
    ZManaged
      .access[DynamoClient](_.get)
      .flatMap(client =>
        withTable("playlists")("id" -> ScalarAttributeType.S)(
          client
        ).as(client)
      )

  def spec =
    suite("script")(
      testM("fail with error message") {
        val input = "select * from playlists limit 1"
        withPlaylistTable
          .use { client =>
            for {
              _ <- insertRows
              () <- Script.eval(Format.Tabular, input)
              output <- TestConsole.output
            } yield {
              assert(output)(
                equalTo(
                  Vector(
                    """id
                  |"456"
                  |""".stripMargin
                  )
                )
              )
            }
          }
      },
      testM("render as json") {
        val input = "select * from playlists limit 2"
        withPlaylistTable.use { client =>
          for {
            _ <- insertRows
            () <- Script.eval(Format.Json, input)
            output <- TestConsole.output
          } yield {
            assert(output)(
              equalTo(
                Vector(
                  """{"id":"456"}
                     |{"id":"123"}
                     |""".stripMargin
                )
              )
            )
          }
        }
      },
      testM("render as json-pretty") {
        withPlaylistTable.use {
          client =>
            val input = "select * from playlists limit 2"
            for {
              _ <- insertRows
              () <- Script.eval(Format.JsonPretty, input)
              output <- TestConsole.output
            } yield {
              assert(output)(
                equalTo(
                  Vector(
                    """{
                      |  "id" : "456"
                      |}
                      |{
                      |  "id" : "123"
                      |}
                      |""".stripMargin
                  )
                )
              )
            }
        }
      },
      testM("render an error when parsing fails") {
        val input = "select * from playlists limid"
        for {
          message <- Script
            .eval(Format.JsonPretty, input)
            .flip
            .map(_.getMessage)
        } yield {
          assert(message)(
            equalTo(
              """[error] Failed to parse query
                     |select * from playlists limid
                     |                        ^""".stripMargin
            )
          )
        }
      },
      testM("create table") {
        val input = "create table users (userId string);"
        (for {
          () <- Script.eval(Format.Tabular, input)
          output <- TestConsole.output
        } yield {
          assert(output)(equalTo(Vector()))
        }).ensuring(cleanupTable("users"))
      },
      testM("support multiple statements") {
        val input =
          """
            |create table users (userId string);
            |create table events (userId string);
            |create table notifications (userId string);
            |""".stripMargin
        (for {
          () <- Script.eval(Format.Tabular, input)
          output <- TestConsole.output
        } yield {
          assert(output)(equalTo(Vector()))
        }).ensuring(cleanupTable("users"))
          .ensuring(cleanupTable("events"))
          .ensuring(cleanupTable("notifications"))
      },
      testM("render table names") {
        withPlaylistTable
          .flatMap(client =>
            withTable("tracks")("id" -> ScalarAttributeType.S)(
              client
            ).as(client)
          )
          .use { client =>
            val input = "show tables"
            for {
              () <- Script.eval(Format.Tabular, input)
              output <- TestConsole.output
            } yield {
              assert(output)(
                equalTo(
                  Vector(
                    """playlists
                  |tracks
                  |""".stripMargin
                  )
                )
              )
            }
          }
      }
//    testM("render paginated table names") {
//      val input = "show tables"
//      val (out, result) = captureStdOut {
//        Script.eval(Opts(), input, client)
//      }
//      assert(result)(isRight(anything))
//      assert(out)(
//        equalTo(
//          """playlists
//          |tracks
//          |""".stripMargin
//        )
//      )
//    },
      //TODO: pass client as layer to allow mocking errors
//    test("render an error when rendering table names fails") {
//      val input = "show tables"
//      val error = "Error occurred while loading list of tables"
//      Script.eval(Opts(), input, client)
//      assert(result)(isLeft(equalTo(error)))
//    }
    ) @@ TestAspect.sequential @@
      TestAspect.before(TestConsole.clearOutput) provideCustomLayerShared (
      (dynamoClient >>> Dynamo.live.passthrough).orDie
    )

}
