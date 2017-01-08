package dynamite

import java.io._

import java.util.concurrent.atomic.AtomicReference
import com.amazonaws.jmespath.ObjectMapperSingleton
import fansi.{ Bold, Str }
import org.scalatest._

import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

class ReplSpec
    extends FlatSpec
    with Matchers
    with EitherValues {

  def withReader[A](query: String)(f: Reader => A) = {
    val reader = new TestReader(query)
    try f(reader) finally {
      reader.close()
    }
  }

  "repl" should "tolerate failed responses" in {
    val query = s"select * from playlists limit 1;"
    val writer = new StringWriter()
    withReader(query) { reader =>
      val repl = new Repl(
        _ => Failure(new Exception("fail")),
        reader,
        new PrintWriter(writer),
        new AtomicReference(Ast.Format.Tabular)
      )
      repl.run()
    }
    writer.toString should be(
      s"""${Repl.formatError("fail")}
        |""".stripMargin
    )
  }

  it should "support aggregates" in {
    val writer = new StringWriter()
    val query = s"select count(*) from playlists where userId = 'user-id-1' ;"
    withReader(query) { reader =>
      val repl = new Repl(
        _ => Try(
          Response.ResultSet(
            Iterator.single(Timed(Success(List {
              val node = ObjectMapperSingleton.getObjectMapper.createObjectNode()
              node.put("count", 20)
            }), 1.second))
          )
        ),
        reader,
        new PrintWriter(writer),
        new AtomicReference(Ast.Format.Tabular)
      ).run()
    }
    writer.toString should be(
      s"""${Bold.On(Str("count"))}
        |20 ${"  "}
        |
        |Completed in 1000 ms
        |""".stripMargin
    )
  }

}
