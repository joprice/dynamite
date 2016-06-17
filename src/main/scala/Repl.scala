package dynamite

import java.io.{ File, PrintWriter }
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.Item
import fastparse.core.Parsed
import jline.console.ConsoleReader
import jline.console.history.FileHistory

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import dynamite.Ast.{ All, ShowTables }
import fansi._
import scala.util.{ Failure, Success, Try }

object Repl {

  def main(args: Array[String]): Unit = {
    parser.parse(args, Opts()) match {
      case Some(config) => startRepl(config)
      case None =>
    }
  }

  case class Opts(endpoint: Option[String] = None)

  sealed trait Paging[A] {
    def pageData: Iterator[Try[List[A]]]
  }

  final class TablePaging(
    val select: Ast.Select,
    val pageData: Iterator[Try[List[Item]]]
  ) extends Paging[Item]

  final class TableNamePaging(val pageData: Iterator[Try[List[String]]]) extends Paging[String]

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

  val parser = new scopt.OptionParser[Opts]("dynamite") {
    opt[String]("endpoint").action((x, c) =>
      c.copy(endpoint = Some(x))).text("aws endpoint")
  }

  def startRepl(opts: Opts) = {
    val reader = new ConsoleReader()

    val history = new FileHistory(new File(sys.props("user.home"), ".dql-history"))
    reader.setHistory(history)
    resetPrompt(reader)
    reader.setExpandEvents(false)
    //reader.setHandleUserInterrupt(true)

    //TODO: autocompletion

    //      val arg = new ArgumentCompleter(new StringsCompleter("select"), new StringsCompleter("from"), new NullCompleter())
    //      arg.setStrict(false)
    //      val completer = new AggregateCompleter(
    //        arg
    //        //new ArgumentCompleter(new StringsCompleter("select"), new NullCompleter()),
    //      )
    //      reader.addCompleter(completer)
    //      reader.addCompleter(
    //        new StringsCompleter("where")
    //      )

    //      val handler = new CandidateListCompletionHandler()
    //      reader.setCompletionHandler(handler)

    val out = new PrintWriter(reader.getOutput)

    class Lazy[A](value: => A) {
      var accessed = false
      private[this] lazy val _value = {
        accessed = true
        value
      }
      def apply() = _value
    }

    val client = new Lazy({
      println("Connecting to Dynamo...")
      dynamoClient(opts.endpoint)
    })
    lazy val eval = Eval(client())

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

  def loop(eval: Ast.Query => Try[Response], reader: ConsoleReader, out: PrintWriter) = {
    def parsed[A](line: String)(f: Ast.Query => A) = {
      Parser(line.trim) match {
        case Parsed.Success(query, _) => f(query)
        case failure: Parsed.Failure => parseError(line, failure)
      }
    }

    def reportError(msg: String) = out.println(s"[${Bold.On(Color.Red(Str("error")))}] $msg")

    def parseError(line: String, failure: Parsed.Failure) = {
      //TODO: improve error output - use fastparse error
      reportError("Failed to parse query")
      out.println(Color.Red(line))
      out.println((" " * failure.index) + "^")
    }

    def run[A](query: Ast.Query)(f: Response => A) = {
      eval(query) match {
        case Success(results) => f(results)
        case Failure(ex) => reportError(ex.getMessage)
      }
    }

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
          paging.pageData.next match {
            case Success(values) =>
              //TODO: if results is less than page size, finish early?
              if (values.nonEmpty) {
                process(values)
              }
              if (!paging.pageData.hasNext) {
                resetPagination()
              }
            case Failure(ex) =>
              resetPagination()
              reportError(ex.getMessage)
          }
        }

        paging match {
          case paging: TablePaging =>
            handle(paging) { values =>
              render(values, paging.select.projection)
            }
          case paging: TableNamePaging =>
            handle(paging) { values =>
              val headers = Seq(Bold.On(Str("name")))
              //TODO: move headers into table
              Table(out, headers +: values.map(name => Seq(Str(name))))
            }
        }
      }
    }

    def report(query: Ast.Query, results: Response) = (query, results) match {
      case (select: Ast.Select, ResultSet(resultSet)) =>
        // TODO: completion: success/failure, time, result count, indicate empty?
        //TODO: add flag with query cost
        paging = new TablePaging(select, resultSet)
        nextPage()
      case (update: Ast.Update, Complete) =>
      //TODO: output for update / delete /ddl
      case (ShowTables, TableNames(resultSet)) =>
        paging = new TableNamePaging(resultSet)
        nextPage()
      case unhandled =>
        out.println(Color.Yellow(s"[warn] unhandled response $unhandled"))
    }

    //TODO: when projecting all fields, show hash/sort keys first?
    def render(list: Seq[Item], select: Ast.Projection) = {
      val headers = select match {
        case All =>
          list.headOption.fold(Seq.empty[String])(_.asMap.asScala.keys.toSeq).sorted
        /*list.headOption.map(Json.parse(_).as[JsObject].keys)
            .fold(Seq.empty[String])(_.toSeq).sorted
            */
        case Ast.Fields(fields) => fields
      }
      //TODO: not all fields need the same names. If All was provided in the query,
      // alphas sort by fields present in all objects, followed by the sparse ones?
      //TODO: match order provided if not star
      val body = list.map { item =>
        val data = item.asMap.asScala
        //val data = Json.parse(item).as[Map[String, JsValue]]
        // missing fields are represented by an empty str
        headers.map {
          header => Str(data.get(header).map(_.toString).getOrElse(""))
        }
      }
      Table(out, headers.map(header => Bold.On(Str(header))) +: body)
    }

    // While paging, a single char is read so that a new line is not required to
    // go the next page, or quit the pager
    def nextChar = if (inPager) {
      reader.readCharacter().toChar.toString
    } else reader.readLine()

    //TODO: rewrite using tailrec?
    var line: String = null

    while ({ line = nextChar; line != null }) {
      reader.setEchoCharacter(0.toChar)
      val trimmed = line.trim
      if (inPager) {
        // q is used to quit pagination
        if (trimmed == "q")
          resetPagination()
        else
          nextPage()
      } else {
        if (trimmed.nonEmpty) {
          //TODO: switch to either with custom error type for console output
          parsed(line.trim) { query =>
            run(query) { results =>
              report(query, results)
            }
          }
        }
      }
      out.flush()
    }
  }
}

