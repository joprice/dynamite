package dynamite

import java.io.File
import scala.util.{ Try, Success }
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

final case class DynamiteConfig(pageSize: Int = 20)

object DynamiteConfig {
  val defaults = DynamiteConfig(20)
}

object Config {

  def ensureConfigDir() = {
    val configDir = new File(sys.props("user.home"), ".dql")
    configDir.mkdirs()
    configDir
  }

  def loadConfig(configDir: File): Try[DynamiteConfig] = {
    val configFile = new File(configDir, "config")
    if (configFile.exists) {
      Try {
        ConfigFactory.parseFile(configFile)
          .as[DynamiteConfig]("dynamite")
      }
    } else {
      Success(DynamiteConfig.defaults)
    }
  }
}

