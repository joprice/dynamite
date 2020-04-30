package dynamite

import dynamite.Ast.{Format => _, _}
import jline.internal.Ansi
import zio._
import zio.config.Config
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import dynamite.Dynamo.DynamoObject
import dynamite.Response.Complete
import zio.console.{putStrLn, Console}
import zio.stream._
import play.api.libs.json.{Format => _, _}
import scala.jdk.CollectionConverters._

object Script {

  // paginate through the results, exiting at the first failure and printing as each page is processed
  def render[R1, E, A](
      data: ZStream[R1, E, Timed[A]]
  )(
      f: (A, Boolean) => ZIO[Any, E, String]
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
      Repl
        .render(
          values,
          projection,
          withHeaders,
          align = false
        )
        .map(result => Ansi.stripAnsi(result).trim)
    case Format.Json =>
      println(s"values $values")
      Task
        .foreach(values)(dynamoObjectToJson)
        .map(_.mkString("\n"))
    case Format.JsonPretty =>
      println(s"values $values")
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

  //TODO: typed error
  def apply(
      input: String
  ): ZIO[Config[DynamiteConfig] with Console with Eval.Env with Has[
    Opts
  ], Throwable, Unit] =
    for {
      opts <- ZIO.access[Has[Opts]](_.get)
      result <- eval(opts, input)
    } yield result

  def eval(opts: Opts, input: String) =
    ZIO.foreach_(input.split(';').map(_.trim).filter(_.nonEmpty))(
      evalLine(opts, _)
    )

  def evalQuery(query: Query, opts: Opts) = {
    //TODO: mutable state
    val tables = new TableCache(Eval.describeTable)
    Eval(query, tables, pageSize = 20).flatMap { results =>
      (query, results) match {
        case (select: Ast.Select, Response.ResultSet(pages, _)) =>
          render(pages) { (values, first) =>
            renderPage(
              opts.format,
              values.data,
              select.projection,
              first
            )
          }
        case (ShowTables, Response.TableNames(names)) =>
          //TODO: format tables output
          render(names)((page, _) => Task.succeed(page.mkString("\n")))
        //TODO: print update/delete success/failure
        case (DescribeTable(_), Response.TableNames(_)) =>
          Task.unit
        case (_, Response.Info(message)) =>
          putStrLn(message).unit
        case (_: Insert, _) | (_, Complete) =>
          Task.unit
        case unhandled =>
          Task.fail(new Exception(s"unhandled response $unhandled"))
      }
    }
  }

  def evalLine(
      opts: Opts,
      input: String
  ): ZIO[Console with Eval.Env, Throwable, Unit] =
    Parser
      .parse(input.trim)
      .fold(
        failure =>
          Task.fail(
            new Exception(Ansi.stripAnsi(Repl.parseError(input, failure)))
          ), {
          case query: Query => evalQuery(query, opts)
          case _: ReplCommand =>
            Task.fail(
              new Exception("repl commands are not supported in script mode")
            )
        }
      )

}
