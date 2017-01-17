package dynamite

import java.io.{ Closeable, File, PrintWriter, StringWriter }

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDB, AmazonDynamoDBClient }
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.fasterxml.jackson.databind.JsonNode
import dynamite.Ast.Projection.{ Aggregate, FieldSelector }
import jline.console.ConsoleReader
import jline.console.history.FileHistory

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import dynamite.Ast._
import dynamite.Parser.ParseException
import dynamite.Response.{ KeySchema, TableDescription }
import fansi._

import scala.collection.breakOut
import scala.util.{ Failure, Success, Try }

object Repl {

  type ResultPage = Paging[Timed[Try[PageType]]]

  def printTableDescription(out: PrintWriter, description: Response.TableDescription): Unit = {
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

  def handlePage[A, B](
    out: PrintWriter,
    paging: Paging[Timed[Try[A]]],
    reader: Reader
  )(process: A => B): Paging[Timed[Try[A]]] = {
    paging match {
      case Paging.Page(Lazy(Timed(value, duration)), next) =>
        value match {
          case Success(values) =>
            //TODO: if results is less than page size, finish early?
            process(values)
            out.println(s"Completed in ${duration.toMillis} ms")
            //TODO: do this next iteration?
            val nextPage = next()
            next() match {
              case Paging.EOF => reader.resetPagination()
              case _ => out.println("Press q to quit, any key to load next page.")
            }
            nextPage
          //case Failure(ex) if Option(ex.getMessage).exists(_.startsWith("The provided key element does not match the schema")) =>
          case Failure(ex) =>
            reader.resetPagination()
            //ex.printStackTrace()
            //println(s"$ex ${ex.getClass} ${ex.getCause}")
            //TODO: file error logger
            out.println(formatError(ex.getMessage))
            Paging.EOF
        }
      case Paging.EOF => Paging.EOF
    }
  }

  def renderPage(
    out: PrintWriter,
    reader: Reader,
    format: Ast.Format,
    paging: Paging.Page[Timed[Try[PageType]]]
  ): ResultPage = {
    reader.clearPrompt()
    reader.disableEcho()

    handlePage(out, paging, reader) {
      case PageType.TablePaging(select, values) =>
        //TODO: offer vertical printing method
        format match {
          case Ast.Format.Json =>
            //TODO: paginate json output to avoid flooding terminal
            values.foreach { value =>
              out.println(Script.prettyPrint(value))
            }
          case Ast.Format.Tabular =>
            out.print(render(
              values,
              select.projection,
              width = Some(reader.terminalWidth)
            ))
        }
      case PageType.TableNamePaging(tableNames) =>
        out.println(Table(
          headers = Seq("name"),
          data = tableNames.map(name => Seq(Str(name))), None
        ))
    }
  }

  def paginate(
    out: PrintWriter,
    reader: Reader,
    format: Ast.Format,
    results: ResultPage
  ): Unit = {
    results match {
      case Paging.EOF =>
      case page @ Paging.Page(_, _) =>
        val paging = renderPage(out, reader, format, page)
        out.flush()
        paging match {
          case Paging.EOF =>
          case _ =>
            // While paging, a single char is read so that a new line is not required to
            // go the next page, or quit the pager
            val line = reader.readCharacter().toChar.toString
            if (line != null) {
              val trimmed = line.trim
              // q is used to quit pagination
              if (trimmed != "q") {
                paginate(out, reader, format, paging)
              }
            }
        }
    }
    reader.resetPagination()
  }

  def report(
    out: PrintWriter,
    reader: Reader,
    format: Ast.Format,
    query: Ast.Command,
    results: Response
  ): Unit = (query, results) match {
    case (select: Ast.Select, Response.ResultSet(resultSet, _)) =>
      // TODO: completion: success/failure, time, result count, indicate empty?
      //TODO: add flag with query cost
      paginate(out, reader, format, Paging.fromIterator(resultSet.map { page =>
        page.copy(
          result = page.result.map { value =>
            PageType.TablePaging(select, value)
          }
        )
      }))
    //  PageType.TablePaging(select, resultSet))
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
      paginate(out, reader, format, Paging.fromIterator(resultSet.map { page =>
        page.copy(
          result = page.result.map(PageType.TableNamePaging)
        )
      }))
    case (_: DescribeTable, description: Response.TableDescription) =>
      printTableDescription(out, description)
    case (_, Response.Info(message)) =>
      out.println(message)
    case unhandled =>
      out.println(Color.Yellow(s"[warn] unhandled response $unhandled"))
  }

  @tailrec final def loop(
    buffer: String,
    out: PrintWriter,
    reader: Reader,
    format: Ast.Format,
    eval: Ast.Query => Try[Response]
  ): Unit = {
    val line = reader.readLine()

    if (line != null) {
      val trimmed = line.trim
      val (updated, continue, newFormat) = if (trimmed.matches("exit *;?")) {
        ("", false, format)
      } else if (trimmed.contains(";")) {
        reader.resetPrompt()
        //TODO: switch to either with custom error type for console output
        val stripped = {
          val s = trimmed.stripSuffix(";")
          if (buffer.nonEmpty) s"$buffer\n$s" else s
        }
        val newFormat = Parser(stripped)
          .fold({ failure =>
            out.println(parseError(stripped, failure))
            format
          }, {
            case ShowFormat =>
              out.println(format)
              format
            case SetFormat(format) =>
              //Success(Response.Info(s"Format set to $format"))
              out.println(s"Format set to $format")
              format
            case query: Query =>
              eval(query) match {
                case Success(results) =>
                  report(out, reader, format, query, results)
                case Failure(ex) =>
                  val msg = Option(ex.getMessage).getOrElse("An unknown error occurred")
                  out.println(formatError(msg))
              }
              format
          })
        ("", true, newFormat)
      } else {
        reader.setPrompt(Bold.On(Str("  -> ")).render)
        (s"$buffer\n$line", true, format)
      }
      out.flush()
      if (continue) {
        loop(updated, out, reader, newFormat, eval)
      }
    }
  }

  def apply(opts: Opts): Unit = {
    val client = new Lazy({
      dynamoClient(opts.endpoint)
    })
    val config = opts.configFile
      .map(DynamiteConfig.parseConfig)
      .getOrElse {
        DynamiteConfig.loadConfig(DynamiteConfig.defaultConfigDir)
      }.recover {
        case error: Throwable =>
          System.err.println(s"Failed to load config: ${error.getMessage}")
          sys.exit(1)
      }.get
    apply(client, config)
  }

  def apply(client: Lazy[AmazonDynamoDB], config: DynamiteConfig): Unit = {
    println(s"${Opts.appName} v${BuildInfo.version}")

    withCloseable(new ConsoleReader) { reader =>
      withFileHistory(config.historyFile, reader) {
        // To increase repl start time, the client is initialize lazily. This save .5 second, which is
        // instead felt by the user when making the first query
        val wrapped = client.map(new DynamoDB(_))

        val tableCache = new TableCache(Eval.describeTable(wrapped(), _))

        def run(query: Ast.Query) = Eval(wrapped(), query, config.pageSize, tableCache)

        val jLineReader = new JLineReader(reader)
        jLineReader.resetPrompt()
        reader.setExpandEvents(false)
        reader.addCompleter(Completer(reader, tableCache, Eval.showTables(wrapped())))

        val out = new PrintWriter(reader.getOutput)

        //TODO: load current format state from config and persist on change
        loop("", out, jLineReader, Ast.Format.Tabular, run)

        if (client.accessed) {
          client().shutdown()
        }
      }
    }
  }

  val ellipsis = "..."
  val maxColumnLength = 40

  def truncate(s: String) = {
    if (s.length > maxColumnLength)
      s.take(maxColumnLength - ellipsis.length) + ellipsis
    else s
  }

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

  def withCloseable[A <: Closeable, B](closeable: A)(f: A => B) = {
    try f(closeable) finally closeable.close()
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

  def withClient[A](opts: Opts)(f: AmazonDynamoDB => A): A = {
    val client = dynamoClient(opts.endpoint)
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }

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
      val data: Map[String, Any] = item.fields().asScala.toList.map { entry =>
        (entry.getKey, entry.getValue)
      }(breakOut)
      // missing fields are represented by an empty str
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

  def dynamoClient(endpoint: Option[String]) = {
    val config = new ClientConfiguration()
      .withConnectionTimeout(1.second.toMillis.toInt)
      .withSocketTimeout(1.second.toMillis.toInt)
    endpoint.fold(new AmazonDynamoDBClient()) { endpoint =>
      new AmazonDynamoDBClient(new BasicAWSCredentials("", ""), config)
        .withEndpoint[AmazonDynamoDBClient](endpoint)
    }
  }

  def errorLine(line: String, errorIndex: Int) = {
    val to = {
      val t = line.indexOf('\n', errorIndex)
      if (t == -1) line.length
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
      case ParseException.ParseError(error) =>
        val (errorIndex, lineWithError) = errorLine(line, error.index)

        //TODO: improve error output - use fastparse error
        s"""${formatError("Failed to parse query")}
           |${Color.Red(lineWithError)}
           |${(" " * errorIndex) + "^"}""".stripMargin
      case error: ParseException.UnAggregatedFieldsError =>
        formatError(error.getMessage)
    }
  }

  def formatError(msg: String) = s"[${Bold.On(Color.Red(Str("error")))}] $msg"

}

