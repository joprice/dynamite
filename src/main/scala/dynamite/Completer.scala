package dynamite

import java.nio.file.{Files, Paths, StandardOpenOption}

import dynamite.Response.TableNames
import jline.console.ConsoleReader
import jline.console.completer._
import Parser._
import fastparse._
import NoWhitespace._
import zio.Task

import scala.util.{Try}
import scala.jdk.CollectionConverters._

object Completer {
  def literal(values: String*) = new StringsCompleter(values: _*)

  val Null = new NullCompleter

  lazy val path = {
    val p = Paths.get("/tmp/dynamite-debug")
    if (!p.toFile.exists()) {
      Files.createFile(p)
    }
    p
  }

  // prints debug to different file to avoid interrupting jline session
  def debug(s: String) =
    Files.write(path, (s + "\n").getBytes(), StandardOpenOption.APPEND)

  class TableNamesCompleter(loadTables: => Try[TableNames])
      extends StringsCompleter {
    //TODO: refresh cache and only cache when not failed, since automatic or manual?
    lazy val tableNames = {
      val names = loadTables
        .map { tables =>
          //TODO: warn on any page failure
          tables.names.toList.flatMap(_.result.toOption).flatten.sorted
        }
        .getOrElse {
          //TODO: warn that fetching tables failed
          Seq.empty
        }
        .asJava
      super.getStrings.addAll(names)
      names
    }

    override def complete(
        buffer: String,
        cursor: Int,
        candidates: java.util.List[CharSequence]
    ) = {
      // dereference to initialize
      tableNames
      super.complete(buffer, cursor, candidates)
    }
  }

  def tableParser[_: P] =
    ((keyword("select") ~/ spaces ~ projections ~ spaces) ~ from).map {
      case (_, table) => table
    }

  class FieldsCompleter(
      reader: ConsoleReader,
      tableCache: TableCache
  ) extends Completer {
    def keyNames(reader: ConsoleReader) = {
      fastparse.parse(reader.getCursorBuffer.buffer.toString, tableParser(_)) match {
        case Parsed.Success(name, _) => Some(name)
        case _                       => None
      }
    }.fold(Seq.empty[String]) { name =>
      zio.Runtime.default
        .unsafeRun(
          tableCache
            .get(name)
        )
        .map { desc => (desc.hash +: desc.range.toSeq).map(_.name) }
        .getOrElse(Seq.empty)
    }

    override def complete(
        buffer: String,
        cursor: Int,
        candidates: java.util.List[CharSequence]
    ) = {
      new StringsCompleter(keyNames(reader): _*)
        .complete(buffer, cursor, candidates)
    }
  }

  def apply(
      reader: ConsoleReader,
      tableCache: TableCache,
      showTables: => Task[TableNames]
  ) = {
    val tableNames = new TableNamesCompleter(
      Try(zio.Runtime.default.unsafeRun(showTables))
    )
    val fields = new FieldsCompleter(reader, tableCache)

    new AggregateCompleter(
      new ArgumentCompleter(
        literal("select"),
        //TODO: autocomplete when this is a column as well: "swallow" the field tokens
        literal("*"),
        literal("from"),
        tableNames,
        //TODO: branch here literal("asc", "desc", "limit"),
        literal("where"),
        fields,
        Null
      ),
      new ArgumentCompleter(
        literal("update"),
        tableNames,
        literal("set"),
        fields,
        literal("where"),
        //fields,
        Null
      ),
      new ArgumentCompleter(
        literal("delete"),
        literal("from"),
        tableNames,
        Null
      ),
      new ArgumentCompleter(
        literal("insert"),
        literal("into"),
        tableNames, //TODO: fields completer, ignoring commas and parentheses
        //literal("values"),
        Null
      ),
      new ArgumentCompleter(
        literal("show"),
        literal("tables"),
        Null
      ),
      new ArgumentCompleter(
        literal("show"),
        literal("format"),
        Null
      ),
      new ArgumentCompleter(
        literal("format"),
        literal("json", "tabular"),
        Null
      ),
      new ArgumentCompleter(
        literal("describe"),
        literal("table"),
        tableNames,
        Null
      )
    )
  }
}
