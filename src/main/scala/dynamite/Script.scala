package dynamite

import dynamite.Ast.{Format => _, _}
import jline.internal.Ansi
import zio._
import zio.config.Config
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import dynamite.Dynamo.DynamoObject
import zio.console.{putStrLn, Console}
import zio.stream._
import play.api.libs.json.{
  JsArray,
  JsBoolean,
  JsNumber,
  JsObject,
  JsString,
  JsValue,
  Json
}
import zio.clock.Clock

import scala.jdk.CollectionConverters._

object Script {

  // paginate through the results, exiting at the first failure and printing as each page is processed
  def render[R1, E, A](
      data: ZStream[R1, E, Timed[List[A]]]
  )(
      f: (Seq[A], Boolean) => ZIO[Any, E, String]
  ): ZIO[R1 with Console, E, Unit] =
    // paginate through the results, exiting at the first failure and printing as each page is processed
    data
      .mapM(item => f(item.result, false))
      .mapM(putStrLn(_))
      .runDrain

  def renderPage(
      format: Format,
      values: Seq[DynamoObject],
      projection: Seq[Ast.Projection],
      withHeaders: Boolean
  ): Task[String] = format match {
    case Format.Tabular =>
        Repl.render(
          values,
          projection,
          withHeaders,
          align = false
        ).map { result =>
        Ansi .stripAnsi(result) .trim
        }
    case Format.Json =>
      Task
        .foreach(values)(dynamoObjectToJson)
        .map(_.mkString("\n"))
    case Format.JsonPretty =>
      Task
        .foreach(values)(dynamoObjectToJson)
        .map(_.map(result => Json.prettyPrint(result)).mkString("\n"))
  }

  def printDynamoObject(value: DynamoObject, pretty: Boolean) =
    for {
      json <- dynamoObjectToJson(value)
    } yield if (pretty) Json.prettyPrint(json) else Json.stringify(json)

  def attributeValueToJson(value: AttributeValue): Task[JsValue] =
    Option(value.getBOOL)
      .map(JsBoolean(_))
      .orElse(Option(value.getS).map(JsString))
      .orElse(Option(value.getN).map(value => JsNumber(BigDecimal(value))))
      .map(Task.succeed(_))
      .orElse(Option(value.getM).map { value =>
        dynamoObjectToJson(value.asScala.toMap)
      })
      .orElse(Option(value.getL).map { value =>
        Task
          .foreach(value.asScala)(item => attributeValueToJson(item))
          .map(JsArray(_))
      })
      .getOrElse {
        Task.fail(new Exception(s"Failed handling value $value"))
      }

  def dynamoObjectToJson(value: DynamoObject): Task[JsValue] =
    Task
      .foreach(value) {
        case (key, value) =>
          attributeValueToJson(value).map(key -> _)
      }
      .map {
        JsObject(_)
      }

  def withClient[A](opts: Opts) =
    ZManaged.make {
      for {
        config <- ZIO.access[Config[DynamiteConfig]](_.get)
      } yield Repl.dynamoClient(config, opts)
    }(client => ZIO.effectTotal(client.shutdown()))

  //TODO: typed error
  def apply(
      opts: Opts,
      input: String
  ): ZIO[Config[DynamiteConfig] with Console with Clock, Throwable, Unit] =
    withClient(opts).use(client => eval(opts, input, client))

  def eval(
      opts: Opts,
      input: String,
      client: AmazonDynamoDBAsync
  ): ZIO[Console with Clock, Throwable, Unit] =
    Parser
      .parse(input.trim.stripSuffix(";"))
      .fold(
        failure =>
          Task.fail(
            new Exception(Ansi.stripAnsi(Repl.parseError(input, failure)))
          ), {
          case query: Query =>
            val tables = new TableCache(Eval.describeTable(client, _))
            Eval(client, query, tables, pageSize = 20).flatMap {
              results =>
                (query, results) match {
                  case (select: Ast.Select, Response.ResultSet(pages, _)) =>
                    render(pages) { (values, first) =>
                      renderPage(
                        opts.format,
                        values,
                        select.projection,
                        first
                      )
                    }
                  case (ShowTables, Response.TableNames(names)) =>
                    //TODO: format tables output
                    render(names) { (page, _) =>
                      Task.succeed(page.mkString("\n"))
                    }
                  //TODO: print update/delete success/failure
                  case (DescribeTable(_), Response.TableNames(_)) =>
                    Task.unit
                  case (_, Response.Info(message)) =>
                    putStrLn(message) *>
                      Task.unit
                  case (_: Insert, _) =>
                    Task.unit
                  case unhandled =>
                    Task.fail(new Exception(s"unhandled response $unhandled"))
                }
            }
          case _: ReplCommand =>
            Task.fail(
              new Exception("repl commands are not supported in script mode")
            )
        }
      )

}
