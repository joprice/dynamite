package dynamite

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBAsync}
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
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner

import scala.jdk.CollectionConverters._

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

  def withTable[T](tableName: String)(
      attributeDefinitions: (String, ScalarAttributeType)*
  )(
      client: AmazonDynamoDB
  ) =
    ZManaged.make {
      createTable(client)(tableName)(attributeDefinitions: _*)
    }(_ => Task(client.deleteTable(tableName)).orDie)

  val randomPort = {
    // arbitrary value to avoid common low number ports
    val minPort = 10000
    zio.random
      .nextInt(65535 - minPort)
      .map(_ + minPort)
  }

  def dynamoServer(port: Int) =
    ZManaged.makeEffect {
      val localArgs = Array("-inMemory", s"-port", s"$port")
      val server = ServerRunner.createServerFromCommandLineArgs(localArgs)
      server.start()
      server
    }(_.stop)

  def dynamoLocalClient(port: Int) =
    ZManaged.makeEffect(
      Repl
        .dynamoClient(
          endpoint = Some(s"http://localhost:$port"),
          credentials = Some(
            new AWSStaticCredentialsProvider(new BasicAWSCredentials("", ""))
          )
        )
    )(_.shutdown())

  val dynamoClient =
    ZManaged
      .fromEffect(randomPort)
      .flatMap(port => dynamoServer(port) *> dynamoLocalClient(port))

  def captureStdOut[A](f: => A): (String, A) = {
    val os = new ByteArrayOutputStream()
    val result = Console.withOut(os)(f)
    val output = new String(os.toByteArray, StandardCharsets.UTF_8)
    (output, result)
  }

  def insertRows(client: AmazonDynamoDBAsync) =
    for {
      _ <- Dynamo.putItem(
        client,
        "playlists",
        Map(
          "id" -> new AttributeValue("123")
        )
      )
      _ <- Dynamo.putItem(
        client,
        "playlists",
        Map(
          "id" -> new AttributeValue("456")
        )
      )
    } yield ()

  val withPlaylistTable =
    dynamoClient
      .flatMap(client =>
        withTable("playlists")("id" -> ScalarAttributeType.S)(
          client
        ).as(client)
      )

  def spec =
    suite("script")(
      testM("fail with error message") {
        val input = "select * from playlists limit 1"
        dynamoClient
          .flatMap(client =>
            withTable("playlists")("id" -> ScalarAttributeType.S)(
              client
            ).as(client)
          )
          .use { client =>
            for {
              _ <- insertRows(client)
              () <- Script.eval(Opts(), input, client)
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
        withPlaylistTable.use {
          client =>
            for {
              _ <- insertRows(client)
              () <- Script.eval(Opts(format = Format.Json), input, client)
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
              _ <- insertRows(client)
              () <- Script.eval(Opts(format = Format.JsonPretty), input, client)
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
        dynamoClient.use {
          client =>
            for {
              message <- Script
                .eval(Opts(format = Format.JsonPretty), input, client)
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
        }
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
              () <- Script.eval(Opts(), input, client)
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
    ) @@ TestAspect.sequential

}
