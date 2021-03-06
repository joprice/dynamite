package dynamite

import java.io.File
import zio.{Task, ZManaged}
import zio.test._
import zio.test.Assertion._

object DynamiteConfigSpec extends DefaultRunnableSpec {

  def deleteFile(file: File): Task[Unit] =
    if (file.exists) {
      for {
        deleted <- if (file.isFile) {
          Task(file.delete())
        } else {
          Task.foreach(file.listFiles().toSeq)(deleteFile(_)) *> Task(
            file.delete()
          )
        }
        () <- if (!deleted) {
          Task.fail(new Exception(s"Failed to delee file $file"))
        } else Task.unit
      } yield ()
    } else Task.unit

  // based on https://github.com/robey/scalatest-mixins/blob/master/src/main/scala/com/twitter/scalatest/TestFolder.scala
  def tempDirectory[A] =
    ZManaged.make(Task {
      val tempFolder = System.getProperty("java.io.tmpdir")
      var folder: File = null
      do {
        folder = new File(tempFolder, "scalatest-" + System.nanoTime)
      } while (!folder.mkdir())
      folder
    })(folder => deleteFile(folder).orDie)

  def spec = suite("config")(
    testM("load default values for empty file")(
      tempDirectory.use { directory =>
        for {
          result <- DynamiteConfig.loadConfig(directory)
          configFile = new File(directory, "config")
          config <- DynamiteConfig.parseConfig(configFile)
        } yield assert(configFile.exists)(isTrue) && assert(config)(
          equalTo(DynamiteConfig())
        )
      }
    ),
    testM("load existing config file")(
      tempDirectory.use { directory =>
        val configFile = new File(directory, "config")
        DynamiteConfig.write(
          configFile,
          """dynamite {
            |  pageSize = 30
            |}
          """.stripMargin
        )
        assertM(DynamiteConfig.loadConfig(directory))(
          equalTo(DynamiteConfig(pageSize = 30))
        )
      }
    )
  )
}
