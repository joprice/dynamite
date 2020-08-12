package dynamite

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import zio.{App, ExitCode, ZIO, ZManaged}
import zio.logging.{log, Logging}

object DynamoDBLocal extends App {

  def dynamoServer(port: Int) =
    ZManaged.makeEffect {
      val localArgs = Array("-inMemory", s"-port", s"$port")
      val server = ServerRunner.createServerFromCommandLineArgs(localArgs)
      server.start()
      server
    }(_.stop)

  val randomPort = {
    // arbitrary value to avoid common low number ports
    val minPort = 10000
    val maxPort = 65535
    zio.random
      .nextIntBetween(minPort, maxPort)
      .map(_ + minPort)
  }

  val logging = Logging.console((_, logEntry) => logEntry)

  def run(args: List[String]) =
    dynamoServer(8080).useForever
      .as(0)
      .foldM(
        error =>
          log
            .throwable("An unhandled error occurred", error)
            .as(ExitCode.failure),
        ZIO.succeed(_)
      )
      .provideCustomLayer(logging)
      .exitCode
}
