package dynamite

import scala.util.Try
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardOpenOption}
import zio.config.typesafe._
import zio.config._, ConfigDescriptor._
import zio.{ZIO}

final case class DynamiteConfig(
    pageSize: Int = 20,
    historyFile: File = DynamiteConfig.defaultHistoryFile,
    endpoint: Option[String] = None
)

object DynamiteConfig {

  val defaultConfigDir = new File(sys.props("user.home"), ".dynamite")

  val configFileName = "config"

  val defaultConfigFile = new File(defaultConfigDir, configFileName)

  val defaultHistoryFile = new File(defaultConfigDir, "history")

  def write(file: File, contents: String) = Try {
    Files.write(
      file.toPath,
      contents.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.CREATE_NEW
    )
  }

  def createDefaultConfig(configFile: File) = {
    System.err.println(
      s"No config file found at ${configFile.getPath}. Creating one with default values."
    )
    val defaults = DynamiteConfig()
    val defaultConfig = s"""dynamite {
                           |  pageSize = ${defaults.pageSize}
                           |  historyFile = ${DynamiteConfig.defaultHistoryFile.getPath}
                           |}
      """.stripMargin
    write(configFile, defaultConfig)
    defaults
  }

  val descriptor = nested("dynamite")(
    (int("pageSize").default(20) |@|
      file("historyFile").default(DynamiteConfig.defaultHistoryFile) |@|
      string("endpoint").optional)(
      DynamiteConfig.apply _,
      DynamiteConfig.unapply _
    )
  )

  def parseConfig(file: File) =
    TypesafeConfigSource
      .fromHoconFile(file)
      .flatMap(in => ZIO.fromEither(read(descriptor.from(in))))
      .mapError(error => new RuntimeException(error))

  def loadConfig(configDir: File): zio.Task[DynamiteConfig] = {
    configDir.mkdirs()
    val configFile = new File(configDir, configFileName)
    if (configFile.exists) {
      parseConfig(configFile)
    } else {
      ZIO.succeed(createDefaultConfig(configFile))
    }
  }

  def load(file: Option[File]) =
    file
      .map(DynamiteConfig.parseConfig)
      .getOrElse {
        DynamiteConfig.loadConfig(DynamiteConfig.defaultConfigDir)
      }
}
