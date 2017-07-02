package dynamite

import com.amazonaws.jmespath.ObjectMapperSingleton
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.fasterxml.jackson.databind.JsonNode
import dynamite.Ast._
import jline.internal.Ansi

import scala.util.{ Failure, Success, Try }

object Script {

  // paginate through the results, exiting at the first failure and printing as each page is processed
  def render[A](
    data: Iterator[Timed[Try[List[A]]]]
  )(f: (Seq[A], Boolean) => String): Either[String, Unit] = {
    // paginate through the results, exiting at the first failure and printing as each page is processed
    var first = true
    var result: Either[String, Unit] = Right(())
    while (result.isRight && data.hasNext) {
      data.next().result match {
        case Success(values) =>
          val output = f(values, first)
          Console.out.println(output)
          result = Right(())
        case Failure(ex) =>
          result = Left(ex.getMessage)
      }
      first = false
    }
    result
  }

  def renderPage(
    format: Format,
    values: Seq[JsonNode],
    projection: Seq[Ast.Projection],
    withHeaders: Boolean
  ) = format match {
    case Format.Tabular =>
      Ansi.stripAnsi(Repl.render(
        values,
        projection,
        withHeaders,
        align = false
      )).trim
    case Format.Json =>
      values.map(_.toString).mkString("\n")
    case Format.JsonPretty =>
      values.map(prettyPrint).mkString("\n")
  }

  def apply(opts: Opts, input: String): Unit = {
    val config = Repl.loadConfig(opts).get
    val endpoint = opts.endpoint.orElse(config.endpoint)
    Repl.withClient(endpoint, None) { client =>
      apply(opts, input, client).left.foreach { reason =>
        Console.err.println(reason)
        sys.exit(1)
      }
    }
  }

  def apply(opts: Opts, input: String, client: AmazonDynamoDB): Either[String, Unit] = {
    Parser.parse(input.trim.stripSuffix(";")).fold({ failure =>
      Left(Ansi.stripAnsi(Repl.parseError(input, failure)))
    }, {
      case query: Query =>
        val dynamo = new DynamoDB(client)
        val tables = new TableCache(Eval.describeTable(dynamo, _))
        Eval(dynamo, query, 20, tables) match {
          case Success(results) =>
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
                  page.mkString("\n")
                }
              //TODO: print update/delete success/failure
              case (DescribeTable(_), Response.TableNames(_)) =>
                Right(())
              case (_, Response.Info(message)) =>
                println(message)
                Right(())
              case unhandled => Left(s"unhandled response $unhandled")
            }
          case Failure(ex) => Left(ex.getMessage)
        }
      case _: ReplCommand =>
        Left("repl commands are not supported in script mode")
    })
  }

  def prettyPrint(node: JsonNode) = {
    val printer = new CustomPrettyPrinter
    ObjectMapperSingleton.getObjectMapper
      .writer(printer)
      .writeValueAsString(node)
  }

}
