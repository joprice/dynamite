package dynamite

import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  ScalarAttributeType
}
import dynamite.Ast.Projection.{Aggregate, FieldSelector}
import dynamite.Dynamo.DynamoObject
import jline.console.ConsoleReader
import jline.console.history.FileHistory
import zio.logging.Logging
import dynamite.Ast._
import dynamite.Parser.ParseException
import dynamite.Response.{Complete, KeySchema, ResultPage}
import fansi._
import zio._
import zio.console.{putStrLn, Console}
import java.io.{Closeable, File, PrintWriter, StringWriter}
import scala.jdk.CollectionConverters._

object Repl {

  def printTableDescription(
      out: PrintWriter,
      description: Response.TableDescription
  ): Unit = {
    def heading(str: String) = Bold.On(Color.LightCyan(Str(str)))

    def renderType(tpe: ScalarAttributeType) = tpe match {
      case ScalarAttributeType.B => "binary"
      case ScalarAttributeType.N => "number"
      case ScalarAttributeType.S => "string"
    }

    def renderSchema(key: KeySchema) =
      Str(s"${key.name} [${Color.LightMagenta(renderType(key.`type`))}]")

    val headers = Seq("name", "hash", "range")
    val indexes = if (description.indexes.nonEmpty) {
      s"""
         |${heading("indexes")}
         |${Table(
           headers,
           description.indexes.map { index =>
             Seq(
               Str(index.name),
               renderSchema(index.hash),
               index.range.fold(Str(""))(renderSchema)
             )
           },
           None
         )}""".stripMargin
    } else ""
    val output =
      s"""${heading("schema")}
         |${Table(
           headers,
           Seq(
             Seq(
               Str(description.name),
               renderSchema(description.hash),
               description.range.fold(Str(""))(renderSchema)
             )
           ),
           None
         )}""".stripMargin + indexes

    out.println(output)
  }

  def printPage[R, A](
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      page: PageType
  ): Task[Unit] =
    page match {
      case PageType.TablePaging(select, values, _) =>
        //TODO: offer vertical printing method
        format match {
          case Ast.Format.Json =>
            //TODO: paginate json output to avoid flooding terminal
            Task
              .foreach_(values) { value =>
                Script
                  .printDynamoObject(value, pretty = true)
                  .flatMap(value => Task(out.println(value)))
              }
          case Ast.Format.Tabular =>
            render(
              values,
              select.projection,
              width = Some(reader.terminalWidth)
            ).map(out.print)
        }
      case PageType.TableNamePaging(tableNames, _) =>
        Task(
          out.println(
            Table(
              headers = Seq("name"),
              data = tableNames.map(name => Seq(Str(name))),
              None
            )
          )
        ).when(tableNames.nonEmpty)
    }
  //TODO: if results is less than page size, finish early?

  def paginate(
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      page: ResultPage
  ): RIO[Eval.Env, Unit] = {
    def waitForConfirmation() =
      for {
        () <- ZIO {
          out.println("Press q to quit, any key to load next page.")
          out.flush()
        }
        line <- Task(reader.readCharacter().toChar.toString)
        // q is used to quit pagination
      } yield {
        if (line != null) {
          line.trim != "q"
        } else true
      }

    page.foreachWhile {
      case Timed(result, duration) =>
        //TODO: don't show q unless another page is available
        for {
          () <- Task(reader.initPagination())
          _ <- printPage(out, reader, format, result)
          () <- ZIO {
            out.println(s"Completed in ${duration.toMillis} ms")
          }
          () <- Task(out.flush())
          // While paging, a single char is read so that a new line is not required to
          // go the next page, or quit the pager
          result <- if (result.hasMore) waitForConfirmation()
          else Task.succeed(false)
        } yield result
    }
  }.ensuring(ZIO {
    reader.resetPagination()
  }.orDie)

  def report(
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      query: Ast.Command,
      results: Response
  ): RIO[Eval.Env, Unit] = (query, results) match {
    case (select: Ast.Select, Response.ResultSet(resultSet, _)) =>
      // TODO: completion: success/failure, time, result count, indicate empty?
      //TODO: add flag with query cost
      paginate(out, reader, format, resultSet.map { page =>
        page.copy(
          result =
            PageType.TablePaging(select, page.result.data, page.result.hasMore)
        )
      })
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
    case (_: Ast.Update, Response.Complete) => Task.unit
    //TODO: output for update / delete /ddl
    case (ShowTables, Response.TableNames(resultSet)) =>
      paginate(out, reader, format, resultSet.map { page =>
        page.copy(
          result = PageType.TableNamePaging(page.result, hasMore = false)
        )
      })
    case (_: DescribeTable, description: Response.TableDescription) =>
      Task(printTableDescription(out, description))
    case (_, Response.Info(message)) =>
      Task(out.println(message))
    case (_: Insert, _) =>
      Task.unit
    case (_, Complete) =>
      Task.unit
    case unhandled =>
      Task(out.println(Color.Yellow(s"[warn] unhandled response $unhandled")))
  }

  final def loop(
      buffer: String,
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      eval: Ast.Query => RIO[Eval.Env, Response]
  ): RIO[Logging with Eval.Env, Unit] = {
    val line = reader.readLine()

    if (line != null) {
      val trimmed = line.trim
      for {
        (updated, continue, newFormat) <- if (trimmed.matches("exit *;?")) {
          Task.succeed(("", false, format))
        } else if (trimmed.contains(";")) {
          reader.resetPrompt()
          //TODO: switch to either with custom error type for console output
          val stripped = {
            val s = trimmed.stripSuffix(";")
            if (buffer.nonEmpty) s"$buffer\n$s" else s
          }
          Parser
            .parse(stripped)
            .fold(
              { failure =>
                out.println(parseError(stripped, failure))
                ZIO.succeed(format)
              }, {
                case ShowFormat =>
                  out.println(format)
                  ZIO.succeed(format)
                case SetFormat(format) =>
                  out.println(s"Format set to $format")
                  ZIO.succeed(format)
                case query: Query =>
                  eval(query)
                    .foldM(
                      ex =>
                        Task {
                          val msg = Option(ex.getMessage)
                            .getOrElse("An unknown error occurred")
                          out.println(formatError(msg))
                        }
                      //TODO: log throwable when verbose flag?
                      //log.throwable("Failed running query", ex)
                      ,
                      results =>
                        report(out, reader, format, query, results)
                          .catchAll { error =>
                            Task {
                              val msg = Option(error.getMessage)
                                .getOrElse("An unknown error occurred")
                              out.println(formatError(msg))
                            }
                          }
                    )
                    .as(format)
              }
            )
            .map(newFormat => ("", true, newFormat))
        } else {
          Task {
            reader.setPrompt(Bold.On(Str("  -> ")).render)
            (s"$buffer\n$line", true, format)
          }
        }
        _ = out.flush()
        () <- if (continue) {
          loop(updated, out, reader, newFormat, eval)
        } else Task.unit
      } yield ()
    } else Task.unit
  }

  def apply(runtime: Runtime[Eval.Env]): ZIO[
    Console with Has[DynamiteConfig] with Logging with Eval.Env,
    Throwable,
    Unit
  ] =
    for {
      config <- ZIO.access[Has[DynamiteConfig]](_.get)
      result <- run(runtime, config)
    } yield result

  val reader = ZManaged.fromAutoCloseable(ZIO.succeed(new ConsoleReader))

  def history(file: File, reader: ConsoleReader) =
    ZManaged.makeEffect {
      val history = new FileHistory(file)
      reader.setHistory(history)
      history
    }(_.flush())

  def run(
      runtime: zio.Runtime[Eval.Env],
      config: DynamiteConfig
  ) =
    reader
      .flatMap(reader => history(config.historyFile, reader).as(reader))
      .use { reader =>
        for {
          _ <- putStrLn(s"${Opts.appName} v${dynamite.BuildInfo.version}")
          result <- {
            val tableCache = new TableCache(Eval.describeTable)

            def run(query: Ast.Query) =
              Eval(query, tableCache, pageSize = config.pageSize)

            val jLineReader = new JLineReader(reader)
            jLineReader.resetPrompt()
            reader.setExpandEvents(false)
            reader.addCompleter(
              Completer(runtime, reader, tableCache, Eval.showTables)
            )

            val out = new PrintWriter(reader.getOutput)

            //TODO: load current format state from config and persist on change
            loop("", out, jLineReader, Ast.Format.Tabular, run)
          }
        } yield result
      }

  val ellipsis = "..."
  val maxColumnLength = 40

  def truncate(s: String) =
    if (s.length > maxColumnLength)
      s.take(maxColumnLength - ellipsis.length) + ellipsis
    else s

  def headers(
      select: Seq[Ast.Projection],
      data: Seq[DynamoObject]
  ): Seq[String] =
    select.flatMap {
      case FieldSelector.All           => data.flatMap(_.keys).distinct.sorted
      case FieldSelector.Field(field)  => Seq(field)
      case count: Aggregate.Count.type => Seq(count.name)
    }

  def withCloseable[A <: Closeable, B](closeable: A)(f: A => B) =
    try f(closeable)
    finally closeable.close()

  def withFileHistory[A](file: File, reader: ConsoleReader)(f: => A) = {
    val history = new FileHistory(file)
    reader.setHistory(history)
    try {
      f
    } finally {
      history.flush()
    }
  }

  def quoteString(s: String) = s""""$s""""

  def renderAttribute(value: AttributeValue): Task[String] =
    Option(value.getBOOL)
      .map(_.toString)
      .orElse(Option(value.getS).map(s => s""""$s""""))
      .orElse(Option(value.getNULL).map(_ => "null"))
      .orElse(Option(value.getN))
      .map(Task.succeed(_))
      .orElse(Option(value.getM).map { value =>
        renderRow(value.asScala.toMap)
          .map(_.map {
            case (key, value) => s""""$key": $value"""
          }.mkString("{", ", ", "}"))
      })
      .orElse(
        Option(value.getL)
          .map(list => Task.foreach(list.asScala.toSeq)(renderAttribute _))
          .orElse(
            Option(value.getSS)
              .map(_.asScala.map(quoteString))
              .orElse(Option(value.getNS).map(_.asScala))
              .map(Task.succeed(_))
          )
          .map(value => value.map(_.mkString("[", ",", "]")))
      )
      //TODO: BS - Binary Set
      // B: binary
      .getOrElse(Task.fail(new Exception(s"Failed to render $value")))

  def renderRow(item: DynamoObject): Task[Map[String, String]] =
    ZIO
      .foreach(item.toSeq) {
        case (key, value) =>
          renderAttribute(value).map(key -> _)
      }
      .map(_.toMap)

  //TODO: when projecting all fields, show hash/sort keys first?
  def render(
      list: Seq[DynamoObject],
      select: Seq[Ast.Projection],
      withHeaders: Boolean = true,
      align: Boolean = true,
      width: Option[Int] = None
  ): Task[String] = {
    val headers = Repl.headers(select, list)
    //TODO: not all fields need the same names. If All was provided in the query,
    // alphas sort by fields present in all objects, followed by the sparse ones?
    //TODO: match order provided if not star
    val body = ZIO.foreach(list) { item =>
      renderRow(item).map { data =>
        // missing fields are represented by an empty str
        headers.map(header => Str(truncate(data.getOrElse(header, ""))))
      }
    }
    body.map { body =>
      if (align) {
        val writer = new StringWriter()
        val out = new PrintWriter(writer)
        out.println(
          Table(
            if (withHeaders) headers else Seq.empty,
            body,
            width
          )
        )
        writer.toString
      } else {
        (headers +: body)
          .map(row => row.mkString("\t"))
          .mkString("\n")
      }
    }
  }

  //TODO: improve connection errors - check dynamo listening on provided port

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

  def parseError(line: String, failure: ParseException) =
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

  def formatError(msg: String) = s"[${Bold.On(Color.Red(Str("error")))}] $msg"

}
