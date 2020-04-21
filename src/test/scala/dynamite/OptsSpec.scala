package dynamite

import zio.test._
import zio.test.Assertion._
import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets

object OptsSpec extends DefaultRunnableSpec {

  def captureStdErr[A](f: => A): (String, A) = {
    val os = new ByteArrayOutputStream()
    val result = Console.withErr(os)(f)
    val output = new String(os.toByteArray, StandardCharsets.UTF_8)
    (output, result)
  }

  def spec = suite("opts")(
    test("fail on unknown option") {
      val (out, _) = captureStdErr {
        Opts.parse(Seq("--invalid=2"))
      }
      assert(out)(startsWithString("Error: Unknown option --invalid=2"))
    },
    test("support config-file option") {
      val result = Opts.parse(Seq("--config-file=/tmp/custom-config"))
      assert(result.flatMap(_.configFile))(
        isSome(equalTo(new File("/tmp/custom-config")))
      )
    },
    test("support endpoint option") {
      val result = Opts.parse(Seq("--endpoint=some-endpoint"))
      assert(result.flatMap(_.endpoint))(isSome(equalTo("some-endpoint")))
    },
    test("support json format option") {
      val result = Opts.parse(Seq("--format=json"))
      assert(result.map(_.format))(isSome(equalTo(Format.Json)))
    },
    test("support json-pretty format option") {
      val result = Opts.parse(Seq("--format=json-pretty"))
      assert(result.map(_.format))(isSome(equalTo(Format.JsonPretty)))
    },
    test("support tabular format option") {
      val result = Opts.parse(Seq("--format=tabular"))
      assert(result.map(_.format))(isSome(equalTo(Format.Tabular)))
    },
    test("fail on invalid format option") {
      val (err, _) = captureStdErr {
        Opts.parse(Seq("--format=unknown"))
      }
      assert(err)(
        startsWithString("Error: Option --format failed when given 'unknown'")
      )
    },
    test("support script option") {
      val result = Opts.parse(Seq("--script=a script"))
      assert(result.flatMap(_.script))(isSome(equalTo("a script")))
    }
  )
}
