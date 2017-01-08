package dynamite

import java.io.{ File, PrintWriter, StringWriter }
import java.util.concurrent.atomic.AtomicReference

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.fasterxml.jackson.databind.JsonNode
import dynamite.Ast.Projection.{ Aggregate, FieldSelector }
import jline.console.ConsoleReader
import jline.console.history.FileHistory

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import dynamite.Ast._
import dynamite.Completer.TableCache
import dynamite.Parser.{ ParseError, ParseException, UnAggregatedFieldsError }
import dynamite.Response.KeySchema
import fansi._

import scala.util.{ Failure, Success, Try }
import com.typesafe.config.ConfigFactory

class Repl(
    eval: Ast.Query => Try[Response],
    reader: Reader,
    out: PrintWriter,
    format: AtomicReference[Ast.Format]
) {
  import Repl._

  var paging: Paging[_] = null

  def inPager = paging != null

  def resetPagination() = {
    paging = null
    reader.enableEcho()
    reader.resetPrompt()
  }

  def nextPage() = {
    if (paging.pageData.hasNext) {
      reader.clearPrompt()
      reader.disableEcho()

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
            } else {
              out.println("Press q to quit, any key to load next page.")
            }
          //case Failure(ex) if Option(ex.getMessage).exists(_.startsWith("The provided key element does not match the schema")) =>
          case Failure(ex) =>
            resetPagination()
            //ex.printStackTrace()
            //println(s"$ex ${ex.getClass} ${ex.getCause}")
            //TODO: file error logger
            out.println(formatError(ex.getMessage))
        }
      }

      paging match {
        case paging: TablePaging =>
          handle(paging) { values =>
            //TODO: offer vetical printing method
            format.get() match {
              case Ast.Format.Json =>
                //TODO: paginate json output to avoid flooding terminal
                values.foreach { value =>
                  out.println(Script.prettyPrint(value))
                }
              case Ast.Format.Tabular =>
                out.print(render(
                  values,
                  paging.select.projection,
                  width = Some(reader.terminalWidth)
                ))
            }
          }
        case paging: TableNamePaging =>
          handle(paging) { values =>
            out.println(Table(
              headers = Seq("name"),
              data = values.map(name => Seq(Str(name))), None
            ))
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
    val output =
      s"""${heading("schema")}
      |${
        Table(
          headers,
          Seq(
            Seq(
              Str(description.name),
              renderSchema(description.hash),
              description.range.fold(Str(""))(renderSchema)
            )
          ),
          None
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
          },
          None
        )
      }""".stripMargin

    out.println(output)
  }

  def report(query: Ast.Query, results: Response) = (query, results) match {
    case (select: Ast.Select, Response.ResultSet(resultSet, _)) =>
      // TODO: completion: success/failure, time, result count, indicate empty?
      //TODO: add flag with query cost
      paging = new TablePaging(select, resultSet)
      nextPage()
    //      capacity.flatMap(value => Option(value())).foreach { cap =>
    //        println(
    //          s"""${cap.getCapacityUnits}
    //               |${cap.getTable}
    //               |${cap.getTableName}
    //               |${cap.getGlobalSecondaryIndexes}
    //               |${cap.getLocalSecondaryIndexes}""".stripMargin
    //        )
    //      }
    case (_: Ast.Update, Response.Complete) =>
    //TODO: output for update / delete /ddl
    case (ShowTables, Response.TableNames(resultSet)) =>
      paging = new TableNamePaging(resultSet)
      nextPage()
    case (_: DescribeTable, description: Response.TableDescription) =>
      printTableDescription(description)
    case (_, Response.Info(message)) =>
      out.println(message)
    case unhandled =>
      out.println(Color.Yellow(s"[warn] unhandled response $unhandled"))
  }

  @tailrec final def loop(buffer: String): Unit = {
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
      } else if (trimmed.matches("exit *;?")) {
        ("", false)
      } else if (trimmed.contains(";")) {
        reader.resetPrompt()
        //TODO: switch to either with custom error type for console output
        val stripped = {
          val s = trimmed.stripSuffix(";")
          if (buffer.nonEmpty) s"$buffer\n$s" else s
        }
        Parser(stripped)
          .fold(failure => out.println(parseError(stripped, failure)), { query =>
            eval(query) match {
              case Success(results) => report(query, results)
              case Failure(ex) =>
                val msg = Option(ex.getMessage).getOrElse("An unknown error occurred")
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
        loop(updated)
      }
    }
  }

  def run() = loop("")

}

object Repl {

  def headers(select: Seq[Ast.Projection], data: Seq[JsonNode]): Seq[String] = {
    select.flatMap {
      case FieldSelector.All =>
        data.flatMap(_.fieldNames().asScala)
          .distinct
          .sorted
      case FieldSelector.Field(field) => Seq(field)
      case count: Aggregate.Count.type => Seq(count.name)
    }
  }

  def apply(opts: Opts): Unit =
    apply(new Lazy({
      dynamoClient(opts.endpoint)
    }))

  def historyFile(configDir: File) =
    new File(configDir, "history")

  def apply(client: Lazy[AmazonDynamoDBClient]): Unit = {
    println(s"${Opts.appName} v${BuildInfo.version}")

    withReader(new ConsoleReader) { reader =>
      val configDir = DynamiteConfig.ensureConfigDir()
      //TODO: log to logfile in .dql
      val config = DynamiteConfig.loadConfig(configDir).recover {
        case error: Throwable =>
          System.err.println(s"Failed to load config: ${error.getMessage}")
          sys.exit(1)
      }.get

      withFileHistory(historyFile(configDir), reader) {
        resetPrompt(reader)
        reader.setExpandEvents(false)

        //TODO: load current state from config
        val format = new AtomicReference[Ast.Format](Ast.Format.Tabular)

        lazy val eval = Eval(client(), config.pageSize, format)

        val tableCache = new TableCache(Lazy(eval.describeTable))
        reader.addCompleter(Completer(reader, eval.showTables(), tableCache))

        // To increase repl start time, the client is initialize lazily. This save .5 second, which is
        // instead felt by the user when making the first query
        def run(query: Ast.Query) = eval.run(query)

        val out = new PrintWriter(reader.getOutput)
        new Repl(run, new JLineReader(reader), out, format).run()

        if (client.accessed) {
          client().shutdown()
        }
      }
    }
  }

  def withReader[A](reader: ConsoleReader)(f: ConsoleReader => A) = {
    try f(reader) finally reader.close()
  }

  def withFileHistory[A](file: File, reader: ConsoleReader)(f: => A) = {
    val history = new FileHistory(file)
    reader.setHistory(history)
    try {
      f
    } finally {
      history.flush()
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

  sealed trait Paging[A] {
    def pageData: Iterator[Timed[Try[List[A]]]]
  }

  final class TablePaging(
    val select: Ast.Select,
    val pageData: Iterator[Timed[Try[List[JsonNode]]]]
  ) extends Paging[JsonNode]

  final class TableNamePaging(
    val pageData: Iterator[Timed[Try[List[String]]]]
  ) extends Paging[String]

  //TODO: when projecting all fields, show hash/sort keys first?
  def render(
    list: Seq[JsonNode],
    select: Seq[Ast.Projection],
    withHeaders: Boolean = true,
    align: Boolean = true,
    width: Option[Int] = None
  ): String = {
    val headers = Repl.headers(select, list)
    //TODO: not all fields need the same names. If All was provided in the query,
    // alphas sort by fields present in all objects, followed by the sparse ones?
    //TODO: match order provided if not star
    val body = list.map { item =>
      import scala.collection.breakOut
      val data: Map[String, Any] = item.fields().asScala.toList.map { entry =>
        (entry.getKey, entry.getValue)
      }(breakOut)
      // missing fields are represented by an empty str
      val maxColumnLength = 40
      val ellipsis = "..."
      def truncate(s: String) = {
        if (s.length > maxColumnLength)
          s.take(maxColumnLength - ellipsis.length) + ellipsis
        else s
      }
      headers.map {
        header =>
          Str(truncate(data.get(header).map {
            // surround strings with quotes to differentiate them from ints
            case s: String => s""""$s""""
            //case list: java.util.ArrayList =>
            //  list.asScala.take(5).toString
            case other => other.toString
          }.getOrElse("")))
      }
    }
    if (align) {
      val writer = new StringWriter()
      val out = new PrintWriter(writer)
      out.println(Table(
        if (withHeaders) headers else Seq.empty,
        body,
        width
      ))
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

  def errorLine(line: String, errorIndex: Int) = {
    val to = {
      val t = line.indexOf('\n', errorIndex)
      if (t == -1) line.size
      else t
    }
    val from = {
      val f = line.lastIndexOf('\n', errorIndex)
      if (f == -1) 0
      else f + 1
    }
    (errorIndex - from, line.substring(from, to))
  }

  def parseError(line: String, failure: ParseException) = {
    failure match {
      case ParseError(error) =>
        val (errorIndex, lineWithError) = errorLine(line, error.index)

        //TODO: improve error output - use fastparse error
        s"""${formatError("Failed to parse query")}
           |${Color.Red(lineWithError)}
           |${(" " * errorIndex) + "^"}""".stripMargin
      case error: UnAggregatedFieldsError =>
        formatError(error.getMessage)
    }
  }

  def formatError(msg: String) = s"[${Bold.On(Color.Red(Str("error")))}] $msg"

}

