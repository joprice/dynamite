package dynamite

import java.util.stream.Collectors

import com.amazonaws.jmespath.ObjectMapperSingleton
import com.fasterxml.jackson.databind.JsonNode
import dynamite.Ast.{ DescribeTable, ShowTables }
import jline.internal.Ansi

import scala.util.{ Failure, Success, Try }

object Script {

  def apply(opts: Opts): Unit = {
    val input = Console.in.lines().collect(Collectors.joining("\n"))
    apply(opts, input)
  }

  // paginate through the results, exiting at the first failure and printing as each page is processed
  def render(
    format: Format,
    projection: Seq[Ast.Projection],
    data: Iterator[Timed[Try[List[JsonNode]]]]
  ) = {
    // paginate through the results, exiting at the first failure and printing as each page is processed
    var first = true
    var result: Either[String, Unit] = Right(())
    while (result.isRight && data.hasNext) {
      data.next().result match {
        case Success(values) =>
          val output = renderPage(
            format,
            values,
            projection,
            first
          )
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
    val result = Parser(input.trim.stripSuffix(";")).fold({ failure =>
      Left(Ansi.stripAnsi(Repl.parseError(input, failure)))
    }, { query =>
      Repl.withClient(opts) { client =>
        Eval(client, pageSize = 20).run(query) match {
          case Success(results) =>
            (query, results) match {
              case (select: Ast.Select, Response.ResultSet(pages, _)) =>
                render(opts.format, select.projection, pages)
              case (ShowTables, Response.TableNames(names)) =>
                //TODO: format tables output
                Console.out.println(names.mkString("\n"))
                Right(())
              //TODO: print update/delete success/failiure
              case (DescribeTable(_), Response.TableNames(_)) => Right(())
              case unhandled => Left(s"unhandled response $unhandled")
            }
          case Failure(ex) => Left(ex.getMessage)
        }
      }
    })

    result.left.foreach { reason =>
      Console.err.println(reason)
      sys.exit(1)
    }
  }

  def prettyPrint(node: JsonNode) =
    ObjectMapperSingleton.getObjectMapper.writerWithDefaultPrettyPrinter()
      .writeValueAsString(node)

}
