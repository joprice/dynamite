package dynamite

import dynamite.Ast.{Format => _, _}
import jline.internal.Ansi

import scala.util.Try
import zio._
import zio.config.Config
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import dynamite.Dynamo.DynamoObject
import zio.console.{Console, putStrLn}
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

import scala.jdk.CollectionConverters._

object Script {

  // paginate through the results, exiting at the first failure and printing as each page is processed
  def render[A](
      data: Iterator[Timed[Try[List[A]]]]
  )(f: (Seq[A], Boolean) => String): RIO[Console, Unit] = {
    // paginate through the results, exiting at the first failure and printing as each page is processed
//    var first = true
//    var result = ZIO.unit
    ZStream
      .fromIterator(ZIO.succeed(data))
      .mapM(item => ZIO.fromTry(item.result))
      .mapM { data => putStrLn(f(data, false)) }
      .runDrain
//    while (result.isRight && data.hasNext) {
//      data.next().result match {
//        case Success(values) =>
//          val output = f(values, first)
//          putStrLn(output)
//          result = Right(())
//        case Failure(ex) =>
//          result = Left(ex.getMessage)
//      }
//      first = false
//    }
//    result
  }

  def renderPage(
      format: Format,
      values: Seq[DynamoObject],
      projection: Seq[Ast.Projection],
      withHeaders: Boolean
  ) = format match {
    case Format.Tabular =>
      Ansi
        .stripAnsi(
          Repl.render(
            values,
            projection,
            withHeaders,
            align = false
          )
        )
        .trim
    case Format.Json =>
      values
        .map(dynamoObjectToJson)
        .mkString("\n")
    case Format.JsonPretty =>
      values
        .map(dynamoObjectToJson)
        .map(Json.prettyPrint)
        .mkString("\n")
  }

  def printDynamoObject(value: DynamoObject, pretty: Boolean) = {
    val json = dynamoObjectToJson(value)
    if (pretty) Json.prettyPrint(json) else Json.stringify(json)
  }

  def attributeValueToJson(value: AttributeValue): JsValue =
    Option(value.getBOOL)
      .map(JsBoolean(_))
      .orElse(Option(value.getS).map(JsString))
      .orElse(Option(value.getN).map(value => JsNumber(BigDecimal(value))))
      .orElse(Option(value.getL).map { value =>
        JsArray(value.asScala.map { item => attributeValueToJson(item) })
      })
      .orElse(Option(value.getM).map { value =>
        dynamoObjectToJson(value.asScala.toMap)
      })
      .getOrElse {
        throw new Exception(s"Failed handling value $value")
      }

  def dynamoObjectToJson(value: DynamoObject): JsValue =
    JsObject(value.view.mapValues(attributeValueToJson).toSeq)

  def withClient[A](opts: Opts) =
    ZManaged.make {
      for {
        // config <- Repl.loadConfig(opts)
        config <- ZIO.access[Config[DynamiteConfig]](_.get)
      } yield Repl.dynamoClient(config, opts)
    }(client => ZIO.effectTotal(client.shutdown()))

  //TODO: typed error
  def apply(
      opts: Opts,
      input: String
  ): ZIO[Config[DynamiteConfig] with Console, Throwable, Unit] =
    withClient(opts).use { client => eval(opts, input, client) }

  def eval(
      opts: Opts,
      input: String,
      client: AmazonDynamoDBAsync
  ): ZIO[Console, Throwable, Unit] =
    Parser
      .parse(input.trim.stripSuffix(";"))
      .fold(
        { failure =>
          Task.fail(
            new Exception(Ansi.stripAnsi(Repl.parseError(input, failure)))
          )
        }, {
          case query: Query =>
            val tables = new TableCache(Eval.describeTable(client, _))
            Eval(client, query, 20, tables).flatMap {
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
                    render(names) { (page, _) => page.mkString("\n") }
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
              // case Failure(ex) => Left(ex.getMessage)
            }
          case _: ReplCommand =>
            Task.fail(
              new Exception("repl commands are not supported in script mode")
            )
        }
      )

}
