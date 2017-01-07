package dynamite

import scopt.Read

final case class Opts(
  endpoint: Option[String] = None,
  format: Format = Format.Tabular,
  script: Option[String] = None
)

object Opts {
  val appName = "dynamite"

  val parser = new scopt.OptionParser[Opts](appName) {
    head(appName, s"v${BuildInfo.version}")
    version("version")

    opt[String]("endpoint").action { (endpoint, config) =>
      config.copy(endpoint = Some(endpoint))
    }.text("aws endpoint")

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
    case "tabular" => Tabular
    case "json" => Json
    case "json-pretty" => JsonPretty
    case value => throw new IllegalArgumentException(s"'$value' is not one of ['json', 'tabular']")
  }
}

