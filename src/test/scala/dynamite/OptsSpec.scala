package dynamite

import java.io.{ ByteArrayOutputStream, File }
import java.nio.charset.StandardCharsets

import org.scalatest._

class OptsSpec
  extends FlatSpec
  with Matchers
  with OptionValues {

  def captureStdErr[A](f: => A): (String, A) = {
    val os = new ByteArrayOutputStream()
    val result = Console.withErr(os)(f)
    val output = new String(os.toByteArray, StandardCharsets.UTF_8)
    (output, result)
  }

  "opts" should "fail on unknown option" in {
    val (out, _) = captureStdErr {
      Opts.parse(Seq("--invalid=2"))
    }
    out should startWith("Error: Unknown option --invalid=2")
  }

  it should "support config-file option" in {
    val result = Opts.parse(Seq("--config-file=/tmp/custom-config")).value
    result.configFile.value shouldBe new File("/tmp/custom-config")
  }

  it should "support endpoint option" in {
    val result = Opts.parse(Seq("--endpoint=some-endpoint")).value
    result.endpoint.value shouldBe "some-endpoint"
  }

  it should "support json format option" in {
    val result = Opts.parse(Seq("--format=json")).value
    result.format shouldBe Format.Json
  }

  it should "support json-pretty format option" in {
    val result = Opts.parse(Seq("--format=json-pretty")).value
    result.format shouldBe Format.JsonPretty
  }

  it should "support tabular format option" in {
    val result = Opts.parse(Seq("--format=tabular")).value
    result.format shouldBe Format.Tabular
  }

  it should "fail on invalid format option" in {
    val (err, _) = captureStdErr {
      Opts.parse(Seq("--format=unknown"))
    }
    err should startWith("Error: Option --format failed when given 'unknown'")
  }

  it should "support script option" in {
    val result = Opts.parse(Seq("--script=a script")).value
    result.script.value shouldBe "a script"
  }

}

