package dynamite

import java.io.{ File, PrintWriter }
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import fastparse.core.Parsed
import jline.console.ConsoleReader
import jline.console.history.FileHistory
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import dynamite.Ast.All
import fansi._
import play.api.libs.json.{ JsObject, JsValue, Json }
import scala.util.{ Failure, Success, Try }
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  val endpoint = opt[String]("endpoint", descr = "aws endpoint")
  verify()
}

object Repl {

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

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val endpoint = conf.endpoint.get
    val client = dynamoClient(endpoint)

    // temporary solution for creating data during interacitve repl testing
    Seed(client)

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
    val eval = Eval(client)

    loop(eval, reader, out)

    history.flush()
    reader.shutdown()
  }

  def loop(eval: Eval, reader: ConsoleReader, out: PrintWriter) = {
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
      eval.run(query) match {
        case Success(results) => f(results)
        case Failure(ex) => reportError(ex.getMessage)
      }
    }

    var paging: Paging = null

    def inPager = paging != null

    class Paging(
      val select: Ast.Select,
      val pageData: Iterator[Try[List[String]]]
    )

    def resetPagination() = {
      paging = null
      reader.setEchoCharacter(null)
      resetPrompt(reader)
    }

    def nextPage() = {
      if (paging.pageData.hasNext) {
        reader.setPrompt("")
        reader.setEchoCharacter(new Character(0))
        paging.pageData.next match {
          case Success(values) =>
            //TODO: if results is less than page size, finish early?
            if (values.nonEmpty) {
              render(values, paging.select.projection)
            }
            if (!paging.pageData.hasNext) {
              resetPagination()
            }
          case Failure(ex) =>
            resetPagination()
            reportError(ex.getMessage)
        }
      }
    }

    def report(query: Ast.Query, results: Response) = (query, results) match {
      case (select: Ast.Select, ResultSet(resultSet)) =>
        // TODO: completion: success/failure, time, result count, indicate empty?
        //TODO: add flag with query cost
        paging = new Paging(select, resultSet)
        nextPage()
      case (update: Ast.Update, Complete) =>
      //TODO: output for update / delete /ddl
      case _ => //invalid
    }

    //TODO: when projecting all fields, show hash/sort keys first?
    def render(list: Seq[String], select: Ast.Projection) = {
      val headers = select match {
        case All =>
          list.headOption.map(Json.parse(_).as[JsObject].keys)
            .fold(Seq.empty[String])(_.toSeq).sorted
        case Ast.Fields(fields) => fields
      }
      //TODO: not all fields need the same names. If All was provided in the query,
      // alphas sort by fields present in all objects, followed by the sparse ones?
      //TODO: match order provided if not star
      val body = list.map { item =>
        val data = Json.parse(item).as[Map[String, JsValue]]
        // missing fields are represented by an empty str
        headers.map {
          header => Str(data.get(header).map(_.toString).getOrElse(""))
        }
      }
      Table(out, headers.map(header => Bold.On(Str(header))) +: body)
    }

    var line: String = null

    // While paging, a single char is read so that a new line is not required to
    // go the next page, or quit the pager
    def nextChar = if (inPager) {
      reader.readCharacter().toChar.toString
    } else reader.readLine()

    while ({ line = nextChar; line != null }) {
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

