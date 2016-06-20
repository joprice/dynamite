package dynamite

import java.io._
import java.nio.charset.StandardCharsets
import org.scalatest._
import scala.util.Failure

class TestReader(line: String) extends Reader {
  val in = new BufferedReader(
    new InputStreamReader(
      new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8))
    )
  )

  def enableEcho(): Unit = ()
  def clearPrompt(): Unit = ()
  def resetPrompt(): Unit = ()
  def setPrompt(prompt: String): Unit = ()
  def disableEcho(): Unit = ()
  def readLine(): String = in.readLine()
  def readCharacter(): Int = ' '
}

class ReplSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with DynamoTestClient {

  //lazy val repl = Repl(Lazy(client))

  "repl" should "work" in {
    val query = "select * from playlists limit 1;"
    val str = new StringWriter()
    new Repl(
      { query =>
        println(s"got query $query")
        Failure(new Exception("fail"))
      },
      new TestReader(query),
      new PrintWriter(str)
    ).run()
  }

  //  it should "allow selecting all fields" in {
  //  }

}
