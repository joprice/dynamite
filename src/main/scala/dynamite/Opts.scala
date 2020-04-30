package dynamite

import java.io.File
import scopt.Read

final case class Opts(
    endpoint: Option[String] = None,
    format: Format = Format.Tabular,
    script: Option[String] = None,
    configFile: Option[File] = None,
    profile: Option[String] = None,
    local : Boolean = false
)

object Opts {
  val appName = "dynamite"

  def parse(args: Seq[String]) = parser.parse(args, Opts())

  val parser = new scopt.OptionParser[Opts](appName) {
    head(appName, s"v${BuildInfo.version}")
    version("version")

    opt[Unit]("local")
      .action { case (_, config) => config.copy(local = true) }
      .text("Whether to start and connect to a local dynamodb instance for testing")

    opt[File]("config-file")
      .action { (configFile, config) =>
        config.copy(configFile = Some(configFile))
      }
      .text("Config file location. Default is $HOME/.dynamite/config")

    opt[String]("endpoint")
      .action((endpoint, config) => config.copy(endpoint = Some(endpoint)))
      .text("aws endpoint")

    opt[String]("profile")
      .action((profile, config) => config.copy(profile = Some(profile)))
      .text("AWS IAM profile")

    opt[Format]("format").action { (render, config) =>
      config.copy(format = render)
    }

    opt[String]("script").action { (script, config) =>
      config.copy(script = Some(script))
    }
  }
}

/**
  * Rendering format for output in non-interactive
  */
sealed abstract class Format extends Product with Serializable

object Format {
  case object Json extends Format
  case object JsonPretty extends Format
  case object Tabular extends Format

  implicit val read: Read[Format] = scopt.Read.reads {
    case "tabular"     => Tabular
    case "json"        => Json
    case "json-pretty" => JsonPretty
    case value =>
      throw new IllegalArgumentException(
        s"'$value' is not one of ['json', 'tabular']"
      )
  }
}
