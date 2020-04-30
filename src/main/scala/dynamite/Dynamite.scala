package dynamite

import java.util.stream.Collectors
import dynamite.Dynamo.Dynamo
import zio._
import zio.logging._

object Dynamite extends App {

  case object ConfigLoadFailure extends Exception("Failed to load config")

  val logging = Logging.console((_, logEntry) => logEntry)

  def optsLayer(args: List[String]): ZLayer[Any, Throwable, Has[Opts]] =
    ZLayer.fromEffect(
      Opts
        .parse(args.toIndexedSeq)
        .map(ZIO.succeed(_))
        .getOrElse(ZIO.fail(ConfigLoadFailure))
    )

  val configLayer =
    ZLayer.fromServiceM((opts: Opts) => DynamiteConfig.load(opts.configFile))

  //TODO: merge Opts and Config to make this simpler
  def layer(
      args: List[String]
  ) = {
    val opts = optsLayer(args)
    val config = opts >>> configLayer.passthrough
    val dynamo = config >>> Dynamo.layer
    (dynamo >>> Dynamo.live) ++ logging ++ opts ++ config
  }

  def run(args: List[String]) =
    (for {
      opts <- ZIO.access[Has[Opts]](_.get)
      dynamo <- ZIO.access[Dynamo](_.get)
      () <- opts.script match {
        case None =>
          if (System.console() == null) {
            val input = Console.in.lines().collect(Collectors.joining("\n"))
            Script(input)
          } else {
            Repl(this.map(_.add(dynamo)))
          }
        case Some(script @ _) => Script(script)
      }
    } yield 0)
      .provideCustomLayer(layer(args))
      .foldM(
        {
          case ConfigLoadFailure =>
            ZIO.succeed(1)
          case error =>
            log
              .throwable("An unhandled error occurred", error)
              .as(1)
        },
        _ => ZIO.succeed(0)
      )
      .provideCustomLayer(logging)
}
