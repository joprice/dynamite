package dynamite

import java.util.stream.Collectors
import zio._
import zio.logging._

object Dynamite extends App {

  val logging = Logging.console((_, logEntry) => logEntry)

  def run(args: List[String]) =
    (
      for {
        opts <- ZIO
          .fromOption(Opts.parse(args.toIndexedSeq))
          .mapError(_ => new Exception("Failed parsing options"))
        config <- DynamiteConfig.load(opts.configFile)
        result <- (opts.script match {
          case None =>
            if (System.console() == null) {
              val input = Console.in.lines().collect(Collectors.joining("\n"))
              Script(opts, input)
            } else {
              Repl(opts)
            }
          case Some(script @ _) => Script(opts, script)
        }).provideSomeLayer[ZEnv with Logging](ZLayer.succeed(config))
      } yield result
    ).foldM({ error =>
        log
          .throwable("An unhandled error occurred", error)
          .as(1)
      }, _ => ZIO.succeed(0))
      .provideCustomLayer(logging)
}
