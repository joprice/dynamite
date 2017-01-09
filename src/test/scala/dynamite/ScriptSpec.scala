package dynamite

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.nio.charset.StandardCharsets

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{ AttributeValue, ScanRequest, ScanResult }
import jline.internal.Ansi
import org.scalatest._
import org.scalamock.scalatest.MockFactory

import scala.collection.JavaConverters._

class ScriptSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {

  def captureStdOut[A](f: => A): (String, A) = {
    val os = new ByteArrayOutputStream()
    val result = Console.withOut(os)(f)
    val output = new String(os.toByteArray, StandardCharsets.UTF_8)
    (output, result)
  }

  //  def withStdIn[A](input: String)(f: => A) = {
  //    val stream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))
  //    Console.withIn(stream)(f)
  //  }

  "repl" should "fail with error message" in {
    val input = "select * from playlists limit 1"
    val client = mock[AmazonDynamoDB]
    (client.scan(_: ScanRequest)).expects(
      new ScanRequest()
        .withTableName("playlists")
        .withLimit(1)
    ).returns(
        new ScanResult()
          .withItems(Seq(
            Map[String, AttributeValue](
              "id" -> new AttributeValue("123")
            ),
            Map[String, AttributeValue](
              "id" -> new AttributeValue("456")
            )
          ).map(_.asJava).asJava)
      )
    val (out, result) = captureStdOut {
      Script(Opts(), input, client)
    }
    result.right.value
    out shouldBe
      """id
        |"123"
        |"456"
        |""".stripMargin
  }

  it should "render as json" in {
    val input = "select * from playlists limit 1"
    val client = mock[AmazonDynamoDB]
    (client.scan(_: ScanRequest)).expects(
      new ScanRequest()
        .withTableName("playlists")
        .withLimit(1)
    ).returns(
        new ScanResult()
          .withItems(Seq(
            Map[String, AttributeValue](
              "id" -> new AttributeValue("123")
            ),
            Map[String, AttributeValue](
              "id" -> new AttributeValue("456")
            )
          ).map(_.asJava).asJava)
      )
    val (out, result) = captureStdOut {
      Script(Opts(format = Format.Json), input, client)
    }
    result.right.value
    out shouldBe
      """{"id":"123"}
        |{"id":"456"}
        |""".stripMargin
  }

  it should "render as json-pretty" in {
    val input = "select * from playlists limit 1"
    val client = mock[AmazonDynamoDB]
    (client.scan(_: ScanRequest)).expects(
      new ScanRequest()
        .withTableName("playlists")
        .withLimit(1)
    ).returns(
        new ScanResult()
          .withItems(Seq(
            Map[String, AttributeValue](
              "id" -> new AttributeValue("123")
            ),
            Map[String, AttributeValue](
              "id" -> new AttributeValue("456")
            )
          ).map(_.asJava).asJava)
      )
    val (out, result) = captureStdOut {
      Script(Opts(format = Format.JsonPretty), input, client)
    }
    result.right.value
    out shouldBe
      """{
        |  "id": "123"
        |}
        |{
        |  "id": "456"
        |}
        |""".stripMargin
  }

  it should "render an error when parsing fails" in {
    val input = "select * from playlists limid"
    val client = mock[AmazonDynamoDB]
    val (out, result) = captureStdOut {
      Script(Opts(format = Format.JsonPretty), input, client)
    }
    out shouldBe empty
    result.left.value shouldBe
      """[error] Failed to parse query
        |select * from playlists limid
        |                        ^""".stripMargin
  }

}