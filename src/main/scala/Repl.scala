package dynamite

import java.io.{ File, PrintWriter, StringWriter }
import java.util.stream.Collectors
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import fastparse.core.Parsed
import jline.console.ConsoleReader
import jline.console.history.FileHistory
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import dynamite.Ast._
import dynamite.Completer.TableCache
import dynamite.Response.KeySchema
import fansi._
import jline.internal.Ansi
import scala.util.{ Failure, Success, Try }

object Repl {

  def main(args: Array[String]): Unit = {
    Opts.parser.parse(args, Opts()) match {
      case Some(config) =>
        if (System.console() == null) {
          runNonInteractive(config)
        } else {
          runInteractive(config)
        }
      case None =>
    }
  }

  def withClient[A](opts: Opts)(f: AmazonDynamoDBClient => A) = {
    val client = dynamoClient(opts.endpoint)
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }

  def runNonInteractive(opts: Opts) = {
    val input = Console.in.lines().collect(Collectors.joining("\n"))
    val result = Parser(input).fold({ failure =>
      Left(Ansi.stripAnsi(parseError(input, failure)))
    }, { query =>
      withClient(opts) { client =>
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
                          Ansi.stripAnsi(render(
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

  sealed trait Paging[A] {
    def pageData: Iterator[Timed[Try[List[A]]]]
  }

  final class TablePaging(
    val select: Ast.Select,
    val pageData: Iterator[Timed[Try[List[Item]]]]
  ) extends Paging[Item]

  final class TableNamePaging(
    val pageData: Iterator[Timed[Try[List[String]]]]
  ) extends Paging[String]

  //TODO: when projecting all fields, show hash/sort keys first?
  def render(
    list: Seq[Item],
    select: Ast.Projection,
    withHeaders: Boolean = true,
    align: Boolean = true
  ): String = {
    val headers = select match {
      case All =>
        list.headOption
          .fold(Seq.empty[String])(_.asMap.asScala.keys.toSeq)
          .sorted
      case Ast.Fields(fields) => fields
    }
    //TODO: not all fields need the same names. If All was provided in the query,
    // alphas sort by fields present in all objects, followed by the sparse ones?
    //TODO: match order provided if not star
    val body = list.map { item =>
      val data = item.asMap.asScala
      // missing fields are represented by an empty str
      headers.map {
        header =>
          Str(data.get(header).map {
            // surround strings with quotes to differentiate them from ints
            case s: String => s""""$s""""
            case other => other.toString
          }.getOrElse(""))
      }
    }
    if (align) {
      val writer = new StringWriter()
      val out = new PrintWriter(writer)
      out.println(Table(if (withHeaders) headers else Seq.empty, body))
      writer.toString
    } else {
      (headers +: body)
        .map(row => row.mkString("\t"))
        .mkString("\n")
    }
  }

  //TODO: improve connection errors - check dynamo listening on provided port

  //TODO: parse port to int
  def dynamoClient(endpoint: Option[String]) = {
    val config = new ClientConfiguration()
      .withConnectionTimeout(1.second.toMillis.toInt)
      .withSocketTimeout(1.second.toMillis.toInt)
    endpoint.fold(new AmazonDynamoDBClient()) { endpoint =>
      new AmazonDynamoDBClient(new BasicAWSCredentials("", ""), config)
        .withEndpoint[AmazonDynamoDBClient](endpoint)
    }
  }

  def resetPrompt(reader: ConsoleReader) =
    reader.setPrompt(Bold.On(Str("dql> ")).render)

  def runInteractive(opts: Opts) = {
    println(s"${Opts.appName} v${BuildInfo.version}")
    val reader = new ConsoleReader()
    val history = new FileHistory(new File(sys.props("user.home"), ".dql-history"))
    reader.setHistory(history)
    resetPrompt(reader)
    reader.setExpandEvents(false)

    val out = new PrintWriter(reader.getOutput)

    val client = new Lazy({
      dynamoClient(opts.endpoint)
    })
    lazy val eval = Eval(client())

    val tableCache = new TableCache(Lazy(eval.describeTable))
    reader.addCompleter(Completer(reader, eval.showTables, tableCache))

    // To increase repl start time, the client is initialize lazily. This save .5 second, which is
    // instead felt by the user when making the first query
    def run(query: Ast.Query) = eval.run(query)

    loop(run, reader, out)

    if (client.accessed) {
      client().shutdown()
    }
    history.flush()
    reader.shutdown()
  }

  def parseError(line: String, failure: Parsed.Failure) = {
    //TODO: improve error output - use fastparse error
    s"""${formatError("Failed to parse query")}
      |${Color.Red(line)}
      |${(" " * failure.index) + "^"}""".stripMargin
  }

  def formatError(msg: String) = s"[${Bold.On(Color.Red(Str("error")))}] $msg"

  def loop(eval: Ast.Query => Try[Response], reader: ConsoleReader, out: PrintWriter) = {
    var paging: Paging[_] = null

    def inPager = paging != null

    def resetPagination() = {
      paging = null
      reader.setEchoCharacter(null)
      resetPrompt(reader)
    }

    def nextPage() = {
      if (paging.pageData.hasNext) {
        reader.setPrompt("")
        reader.setEchoCharacter(new Character(0))

        def handle[A, B](paging: Paging[A])(process: List[A] => B) = {
          val Timed(value, duration) = paging.pageData.next
          value match {
            case Success(values) =>
              //TODO: if results is less than page size, finish early?
              if (values.nonEmpty) {
                process(values)
              }
              out.println(s"Completed in ${duration.toMillis} ms")
              if (!paging.pageData.hasNext) {
                resetPagination()
              }
            case Failure(ex) =>
              resetPagination()
              out.println(formatError(ex.getMessage))
          }
        }

        paging match {
          case paging: TablePaging =>
            handle(paging) { values =>
              out.print(render(values, paging.select.projection))
            }
          case paging: TableNamePaging =>
            handle(paging) { values =>
              val headers = Seq("name")
              out.println(Table(headers, values.map(name => Seq(Str(name)))))
            }
        }
      }
    }

    def printTableDescription(description: Response.TableDescription) = {
      def heading(str: String) = Bold.On(Color.LightCyan(Str(str)))

      def renderType(tpe: ScalarAttributeType) = tpe match {
        case ScalarAttributeType.B => "binary"
        case ScalarAttributeType.N => "number"
        case ScalarAttributeType.S => "string"
      }

      def renderSchema(key: KeySchema) =
        Str(s"${key.name} [${Color.LightMagenta(renderType(key.`type`))}]")

      val headers = Seq("name", "hash", "range")

      //TODO: change to one println?
      val output = s"""${heading("schema")}
      |${
        Table(
          headers,
          Seq(
            Seq(
              Str(description.name),
              renderSchema(description.hash),
              description.range.fold(Str(""))(renderSchema)
            )
          )
        )
      }
      |${heading("indexes")}
      |${
        Table(
          headers,
          description.indexes.map { index =>
            Seq(
              Str(index.name),
              renderSchema(index.hash),
              index.range.fold(Str(""))(renderSchema)
            )
          }
        )
      }""".stripMargin

      out.println(output)
    }

    def report(query: Ast.Query, results: Response) = (query, results) match {
      case (select: Ast.Select, Response.ResultSet(resultSet, capacity)) =>
        // TODO: completion: success/failure, time, result count, indicate empty?
        //TODO: add flag with query cost
        paging = new TablePaging(select, resultSet)
        nextPage()
        capacity.flatMap(value => Option(value())).foreach { cap =>
          println(
            s"""${cap.getCapacityUnits}
               |${cap.getTable}
               |${cap.getTableName}
               |${cap.getGlobalSecondaryIndexes}
               |${cap.getLocalSecondaryIndexes}""".stripMargin
          )
        }
      case (update: Ast.Update, Response.Complete) =>
      //TODO: output for update / delete /ddl
      case (ShowTables, Response.TableNames(resultSet)) =>
        paging = new TableNamePaging(resultSet)
        nextPage()
      case (_: DescribeTable, description: Response.TableDescription) =>
        printTableDescription(description)
      case unhandled =>
        out.println(Color.Yellow(s"[warn] unhandled response $unhandled"))
    }

    @tailrec def repl(buffer: String): Unit = {
      // While paging, a single char is read so that a new line is not required to
      // go the next page, or quit the pager
      val line = if (inPager) {
        reader.readCharacter().toChar.toString
      } else reader.readLine()
      if (line != null) {
        val trimmed = line.trim
        val (updated, continue) = if (inPager) {
          // q is used to quit pagination
          if (trimmed == "q")
            resetPagination()
          else
            nextPage()
          ("", true)
        } else if (trimmed == "quit") {
          ("", false)
        } else if (trimmed.contains(";")) {
          resetPrompt(reader)
          //TODO: switch to either with custom error type for console output
          val stripped = s"$buffer\n${line.stripSuffix(";")}"
          Parser(stripped)
            .fold(failure => out.println(parseError(stripped, failure)), { query =>
              eval(query) match {
                case Success(results) => report(query, results)
                case Failure(ex) =>
                  val msg = Option(ex.getMessage).getOrElse("An unknown error occurred")
                  ex.printStackTrace()
                  out.println(formatError(msg))
              }
            })
          ("", true)
        } else {
          reader.setPrompt(Bold.On(Str("  -> ")).render)
          (s"$buffer\n$line", true)
        }
        out.flush()
        if (continue) {
          repl(updated)
        }
      }
    }

    repl("")
  }

}

