package dynamite

import java.util.stream.Collectors
import dynamite.Ast.{ DescribeTable, ShowTables }
import jline.internal.Ansi
import scala.util.{ Failure, Success }

object Script {

  def apply(opts: Opts): Unit = {
    val input = Console.in.lines().collect(Collectors.joining("\n"))
    apply(opts, input)
  }

  def apply(opts: Opts, input: String): Unit = {
    val result = Parser(input.trim.stripSuffix(";")).fold({ failure =>
      Left(Ansi.stripAnsi(Repl.parseError(input, failure)))
    }, { query =>
      Repl.withClient(opts) { client =>
        Eval(client).run(query) match {
          case Success(results) =>
            (query, results) match {
              case (select: Ast.Select, Response.ResultSet(pages, capacity)) =>
                // paginate through the results, exiting at the first failure and printing as each page is processed
                var result: Either[String, Unit] = Right(())
                var first = true
                while (result.isRight && pages.hasNext) {
                  val Timed(value, _) = pages.next
                  value match {
                    case Success(values) =>
                      val output = opts.render match {
                        case Format.Tabular =>
                          Ansi.stripAnsi(Repl.render(
                            values,
                            select.projection,
                            withHeaders = first,
                            align = false
                          )).trim
                        case Format.Json =>
                          values.map(_.toJSON).mkString("\n")
                        case Format.JsonPretty =>
                          values.map(_.toJSONPretty).mkString("\n")
                      }
                      Console.out.println(output)
                      first = false
                    case Failure(ex) =>
                      result = Left(ex.getMessage)
                  }
                }
                result
              //TODO: write
              case (ShowTables, Response.TableNames(names)) =>
                Console.out.println(names.mkString("\n"))
                Right(())
              //TODO: print update/delete success/failiure
              case (DescribeTable(_), Response.TableNames(names)) => Right(())
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
}