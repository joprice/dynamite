package dynamite

import java.io.File
import java.nio.file.Files

import org.scalatest._

object DynamiteConfigSpec {

  // based on https://github.com/robey/scalatest-mixins/blob/master/src/main/scala/com/twitter/scalatest/TestFolder.scala
  def withTempDirectory[A](f: File => A): A = {
    def deleteFile(file: File) {
      if (!file.exists) return
      if (file.isFile) {
        file.delete()
      } else {
        file.listFiles().foreach(deleteFile)
        file.delete()
      }
    }
    val tempFolder = System.getProperty("java.io.tmpdir")
    var folder: File = null
    do {
      folder = new File(tempFolder, "scalatest-" + System.nanoTime)
    } while (!folder.mkdir())
    try {
      f(folder)
    } finally {
      deleteFile(folder)
    }
  }
}

class DynamiteConfigSpec
  extends FlatSpec
  with Matchers
  with TryValues
  with EitherValues {
  import DynamiteConfigSpec._

  "config" should "load default values for empty file" in {
    withTempDirectory { directory =>
      val result = DynamiteConfig.loadConfig(directory).success.value
      val configFile = new File(directory, "config")
      configFile should exist
      DynamiteConfig.parseConfig(configFile).success.value should be(DynamiteConfig())
      result should be(DynamiteConfig())
    }
  }

  it should "load existing config file" in {
    withTempDirectory { directory =>
      val configFile = new File(directory, "config")
      DynamiteConfig.write(
        configFile,
        """dynamite {
          |  pageSize = 30
          |}
        """.stripMargin
      )
      val result = DynamiteConfig.loadConfig(directory).success.value
      result should be(DynamiteConfig(pageSize = 30))
    }
  }

}

