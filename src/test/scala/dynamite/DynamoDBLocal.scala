package dynamite

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner
import zio.{App, ZIO, ZManaged}
import zio.logging.{log, Logging}

object DynamoTestHelpers {
  def dynamoServer(port: Int) =
    ZManaged.makeEffect {
      val localArgs = Array("-inMemory", s"-port", s"$port")
      val server = ServerRunner.createServerFromCommandLineArgs(localArgs)
      server.start()
      server
    }(_.stop)

}

object DynamoDBLocal extends App {

  val logging = Logging.console((_, logEntry) => logEntry)

  def run(args: List[String]) =
    DynamoTestHelpers
      .dynamoServer(8080)
      .useForever
      .as(0)
      .foldM({
        case error =>
          log
            .throwable("An unhandled error occurred", error)
            .as(1)
      }, ZIO.succeed(_))
      .provideCustomLayer(logging)
}
