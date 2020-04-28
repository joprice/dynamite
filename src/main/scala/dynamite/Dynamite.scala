package dynamite

import java.util.stream.Collectors
import dynamite.Dynamo.Dynamo
import zio._
import zio.logging._

object Dynamite extends App {

  val logging = Logging.console((_, logEntry) => logEntry)
  def optsLayer(args: List[String]): ZLayer[Any, Throwable, Has[Opts]] =
    ZLayer.fromEffect(
      Opts
        .parse(args.toIndexedSeq)
        .map(ZIO.succeed(_))
        .getOrElse(ZIO.fail(new Exception("Failed to load config")))
    )

  val configLayer =
    ZLayer.fromFunctionM((opts: Has[Opts]) =>
      DynamiteConfig.load(opts.get.configFile)
    )

  //TODO: merge Opts and Config to make this simpler
  def layer(
      args: List[String]
  ) = {
    val opts = optsLayer(args)
    val config = opts >>> configLayer.passthrough
    val dynamo = config >>> Dynamo.layer
    (dynamo >>> Dynamo.live) ++ logging ++ opts ++ config
  }.orDie

  def run(args: List[String]) =
    (for {
      opts <- ZIO.access[Has[Opts]](_.get)
      dynamo <- ZIO.access[Dynamo](_.get)
      result <- opts.script match {
        case None =>
          if (System.console() == null) {
            val input = Console.in.lines().collect(Collectors.joining("\n"))
            Script(input)
          } else {
            Repl(this.map(_ ++ Has(dynamo)))
          }
        case Some(script @ _) => Script(script)
      }
    } yield result)
      .foldM(
        error =>
          log
            .throwable("An unhandled error occurred", error)
            .as(1),
        _ => ZIO.succeed(0)
      )
      .provideCustomLayer(layer(args))
}
