package dynamite

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{ Files, StandardOpenOption }

import scala.util.{ Success, Try }
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.ValueReader

final case class DynamiteConfig(
  pageSize: Int = 20,
  historyFile: File = DynamiteConfig.defaultHistoryFile
)

object DynamiteConfig {

  implicit val fileValueReader: ValueReader[File] = implicitly[ValueReader[String]].map {
    new File(_)
  }

  val defaultConfigDir = new File(sys.props("user.home"), ".dynamite")

  val configFileName = "config"

  val defaultConfigFile = new File(defaultConfigDir, configFileName)

  val defaultHistoryFile = new File(defaultConfigDir, "history")

  def write(file: File, contents: String) = Try {
    Files.write(file.toPath, contents.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW)
  }

  def createDefaultConfig(configFile: File) = {
    System.err.println(s"No config file found at ${configFile.getPath}. Creating one with default values.")
    val defaults = DynamiteConfig()
    val defaultConfig = s"""dynamite {
                           |  pageSize = ${defaults.pageSize}
                           |  historyFile = ${DynamiteConfig.defaultHistoryFile.getPath}
                           |}
      """.stripMargin
    write(configFile, defaultConfig)
    defaults
  }

  def parseConfig(file: File) =
    Try {
      ConfigFactory.parseFile(file)
        .as[DynamiteConfig]("dynamite")
    }

  def loadConfig(configDir: File): Try[DynamiteConfig] = {
    configDir.mkdirs()
    val configFile = new File(configDir, configFileName)
    if (configFile.exists) {
      parseConfig(configFile)
    } else {
      Success(createDefaultConfig(configFile))
    }
  }
}

