package dynamite

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.dynamodbv2.model.{
  AttributeValue,
  ScalarAttributeType
}
import dynamite.Ast.Projection.{Aggregate, FieldSelector}
import dynamite.Dynamo.DynamoObject
import jline.console.ConsoleReader
import jline.console.history.FileHistory
import zio.logging.Logging

import scala.concurrent.duration._
import dynamite.Ast._
import dynamite.Parser.ParseException
import dynamite.Response.{KeySchema, Paged, ResultPage}
import fansi._
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.auth.AWSCredentialsProvider
import zio._
import zio.console.{Console, putStrLn}
import zio.config.Config
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder
import java.io.{Closeable, File, PrintWriter, StringWriter}

import zio.clock.Clock
import zio.stream.ZStream

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
         )}
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

    out.println(output)
  }

  def handlePage[R, A](
      out: PrintWriter,
      page: Paged[R, A],
      reader: Reader
  )(process: A => RIO[R, Any]): RIO[R, Paged[R, A]] = {
    page.runHead
      .foldM(
        { error =>
          //        //TODO: if results is less than page size, finish early?
          //      //case Failure(ex) if Option(ex.getMessage).exists(_.startsWith("The provided key element does not match the schema")) =>
          Task {
            val message =
              Option(error.getMessage)
                .getOrElse("Failed loading page")
            reader.resetPagination()
            //ex.printStackTrace()
            //println(s"$ex ${ex.getClass} ${ex.getCause}")
            //TODO: file error logger
            out.println(formatError(message))
            ZStream.empty
          }
        }, {
          case Some(Timed(values, duration)) =>
            for {
              _ <- process(values)
              _ <- ZIO {
                out.println(s"Completed in ${duration.toMillis} ms")
              }
              //TODO: do this next iteration?
              tail = page.drop(1)
              result <- tail.runHead
                .catchAll(_ => Task.succeed(None))
                .flatMap {
                  case Some(element) =>
                    ZIO(
                      out.println("Press q to quit, any key to load next page.")
                    ) *> ZIO
                      .succeed(ZStream(element) ++ tail.drop(1))
                  case None =>
                    ZIO
                      .succeed(tail)
                }
            } yield result
          case None => ZIO.succeed(ZStream.empty)
        }
      )
  }

  def renderPage(
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      paging: ResultPage
  ): RIO[Clock, ResultPage] = {
    reader.clearPrompt()
    reader.disableEcho()

    handlePage[Clock, PageType](out, paging, reader) {
      case PageType.TablePaging(select, values) =>
        //TODO: offer vertical printing method
        format match {
          case Ast.Format.Json =>
            //TODO: paginate json output to avoid flooding terminal
            Task
              .foreach(values) { value =>
                Script
                  .printDynamoObject(value, pretty = true)
                  .flatMap { value => Task(out.println(value)) }
              }
          case Ast.Format.Tabular =>
            Task {
              out.print(
                render(
                  values,
                  select.projection,
                  width = Some(reader.terminalWidth)
                )
              )
            }
        }
      case PageType.TableNamePaging(tableNames) =>
        Task {
          out.println(
            Table(
              headers = Seq("name"),
              data = tableNames.map(name => Seq(Str(name))),
              None
            )
          )
        }
    }
  }

  def paginate(
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      page: ResultPage
  ): RIO[Clock, Unit] = {
    for {
      paging <- renderPage(out, reader, format, page)
      () <- Task(out.flush())
      result <- paging.runHead.flatMap {
        case None    => Task.unit
        case Some(_) =>
          // While paging, a single char is read so that a new line is not required to
          // go the next page, or quit the pager
          val line = reader.readCharacter().toChar.toString
          if (line != null) {
            val trimmed = line.trim
            // q is used to quit pagination
            if (trimmed != "q") {
              paginate(out, reader, format, paging)
            } else Task.unit
          } else Task.unit
      }
    } yield result
  }.ensuring(ZIO {
    reader.resetPagination()
  }.orDie)

  def report(
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      query: Ast.Command,
      results: Response
  ): RIO[Clock, Unit] = (query, results) match {
    case (select: Ast.Select, Response.ResultSet(resultSet, _)) =>
      // TODO: completion: success/failure, time, result count, indicate empty?
      //TODO: add flag with query cost
      paginate(out, reader, format, resultSet.map { page =>
        page.copy(
          result = PageType.TablePaging(select, page.result)
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
          result = PageType.TableNamePaging(page.result)
        )
      })
    case (_: DescribeTable, description: Response.TableDescription) =>
      Task(printTableDescription(out, description))
    case (_, Response.Info(message)) =>
      Task(out.println(message))
    case (_: Insert, _) =>
      Task.unit
    case unhandled =>
      Task(out.println(Color.Yellow(s"[warn] unhandled response $unhandled")))
  }

  final def loop(
      buffer: String,
      out: PrintWriter,
      reader: Reader,
      format: Ast.Format,
      eval: Ast.Query => Task[Response]
  ): RIO[Logging with Clock, Unit] = {
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
                      { ex =>
                        Task {
                          val msg = Option(ex.getMessage)
                            .getOrElse("An unknown error occurred")
                          out.println(formatError(msg))
                        }
                      //TODO: log throwable when verbose flag?
                      //log.throwable("Failed running query", ex)
                      }, { results =>
                        report(out, reader, format, query, results)
                          .catchAll { error =>
                            Task {
                              val msg = Option(error.getMessage)
                                .getOrElse("An unknown error occurred")
                              out.println(formatError(msg))
                            }
                          }
                      }
                    )
                    .as(format)
              }
            )
            .map { newFormat => ("", true, newFormat) }
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

  def apply(
      opts: Opts
  ): ZIO[Console with Config[DynamiteConfig] with Logging with Clock, Throwable, Unit] =
    Script.withClient(opts).use { client =>
      for {
        config <- ZIO.access[Config[DynamiteConfig]](_.get)
        result <- run(client, config)
      } yield result
    }

  val reader = ZManaged.fromAutoCloseable(ZIO.succeed(new ConsoleReader))

  def history(file: File, reader: ConsoleReader) =
    ZManaged.makeEffect({
      val history = new FileHistory(file)
      reader.setHistory(history)
      history
    })(_.flush())

  def run(client: AmazonDynamoDBAsync, config: DynamiteConfig) =
    reader
      .flatMap(reader => history(config.historyFile, reader).as(reader))
      .use { reader =>
        for {
          _ <- putStrLn(s"${Opts.appName} v${dynamite.BuildInfo.version}")
          result <- {
            val tableCache = new TableCache(Eval.describeTable(client, _))

            def run(query: Ast.Query) =
              Eval(client, query, tableCache, pageSize = config.pageSize)

            val jLineReader = new JLineReader(reader)
            jLineReader.resetPrompt()
            reader.setExpandEvents(false)
            reader.addCompleter(
              Completer(reader, tableCache, Eval.showTables(client))
            )

            val out = new PrintWriter(reader.getOutput)

            //TODO: load current format state from config and persist on change
            loop("", out, jLineReader, Ast.Format.Tabular, run)
          }
        } yield result
      }

  val ellipsis = "..."
  val maxColumnLength = 40

  def truncate(s: String) = {
    if (s.length > maxColumnLength)
      s.take(maxColumnLength - ellipsis.length) + ellipsis
    else s
  }

  def headers(
      select: Seq[Ast.Projection],
      data: Seq[DynamoObject]
  ): Seq[String] = {
    select.flatMap {
      case FieldSelector.All           => data.flatMap(_.keys).distinct.sorted
      case FieldSelector.Field(field)  => Seq(field)
      case count: Aggregate.Count.type => Seq(count.name)
    }
  }

  def withCloseable[A <: Closeable, B](closeable: A)(f: A => B) = {
    try f(closeable)
    finally closeable.close()
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

  def quoteString(s: String) = s""""$s""""

  def renderAttribute(value: AttributeValue): String =
    Option(value.getBOOL)
      .map(_.toString)
      .orElse(Option(value.getS).map(s => s""""$s""""))
      .orElse(
        Option(value.getL)
          .map { list => list.asScala.map(renderAttribute) }
          .orElse(
            Option(value.getSS)
              .map(_.asScala.map(quoteString))
              .orElse(Option(value.getNS).map(_.asScala))
          )
          .map(_.mkString("[", ",", "]"))
      )
      .orElse(Option(value.getNULL).map(_ => "null"))
      .orElse(Option(value.getM).map { value =>
        renderRow(value.asScala.toMap)
          .map {
            case (key, value) => s""""$key": $value"""
          }
          .mkString("{", ", ", "}")
      })
      .orElse(Option(value.getN))
      //TODO: BS - Binary Set
      // B: binary
      .getOrElse(throw new Exception(s"Failed to render $value"))

  def renderRow(item: DynamoObject): Map[String, String] =
    item.view.mapValues(renderAttribute).toMap

  //TODO: when projecting all fields, show hash/sort keys first?
  def render(
      list: Seq[DynamoObject],
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
      val data = renderRow(item)
      // missing fields are represented by an empty str
      headers.map(header => Str(truncate(data.getOrElse(header, ""))))
    }
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

  //TODO: improve connection errors - check dynamo listening on provided port

  def dynamoClient(config: DynamiteConfig, opts: Opts): AmazonDynamoDBAsync = {
    val endpoint = opts.endpoint.orElse(config.endpoint)
    val credentials = opts.profile.map {
      new ProfileCredentialsProvider(_)
    }
    dynamoClient(endpoint, credentials)
  }

  def dynamoClient(
      endpoint: Option[String],
      credentials: Option[AWSCredentialsProvider]
  ): AmazonDynamoDBAsync = {
    val clientConfig = new ClientConfiguration()
      .withConnectionTimeout(1.second.toMillis.toInt)
      .withSocketTimeout(1.second.toMillis.toInt)
    val builder = AmazonDynamoDBAsyncClientBuilder
      .standard()
      .withClientConfiguration(clientConfig)
    val withEndpoint = endpoint.fold(builder) { endpoint =>
      builder.withEndpointConfiguration(
        new EndpointConfiguration(endpoint, builder.getRegion)
      )
    }
    credentials
      .fold(withEndpoint) { credentials =>
        withEndpoint.withCredentials(credentials)
      }
      .build()
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
