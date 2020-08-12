package dynamite

import java.util.stream.Collectors
import dynamite.Dynamo.Dynamo
import zio._
import zio.logging._

object Dynamite extends App {

  case object ConfigLoadFailure extends Exception("Failed to load config")

  val logging = Logging.console((_, logEntry) => logEntry)

  def loadOpts(args: List[String]) =
    Opts
      .parse(args.toIndexedSeq)
      .map(ZIO.succeed(_))
      .getOrElse(ZIO.fail(ConfigLoadFailure))

  def optsLayer(args: List[String]): ZLayer[Any, Throwable, Has[Opts]] =
    ZLayer.fromEffect(loadOpts(args))

  val configLayer =
    ZLayer.fromServiceM((opts: Opts) => DynamiteConfig.load(opts.configFile))

  //TODO: merge Opts and Config to make this simpler
  def layer(opts: Opts) = {
    val optsLayer = ZLayer.succeed(opts)
    val config = optsLayer >>> configLayer.passthrough
    val dynamoClient = config >>> Dynamo.clientLayer
    (dynamoClient >>> Dynamo.live) ++ logging ++ optsLayer ++ config
  }

  def runDynamoDBLocal(opts: Opts) =
    (
      for {
        dynamodbLocalPort <- ZManaged.fromEffect(DynamoDBLocal.randomPort)
        dynamodbLocal <- if (opts.local) {
          DynamoDBLocal
            .dynamoServer(dynamodbLocalPort)
            .map(Some(_))
        } else ZManaged.succeed(None)
      } yield dynamodbLocal.map(_ -> dynamodbLocalPort)
    ).map(_.map(_._2))

  def eval(script: Option[String]) =
    script match {
      case None =>
        if (System.console() == null) {
          for {
            input <- Task(Console.in.lines().collect(Collectors.joining("\n")))
            result <- Script(input)
          } yield result
        } else {
          for {
            dynamo <- ZIO.access[Dynamo](_.get)
            result <- Repl(this.map(_.add(dynamo)))
          } yield result
        }
      case Some(script) => Script(script)
    }

  def run(args: List[String]) =
    loadOpts(args)
      .flatMap { opts =>
        runDynamoDBLocal(opts)
          .use { maybePort =>
            val newOpts = maybePort.fold(opts) { port =>
              opts.copy(endpoint = Some(s"http://localhost:$port"))
            }
            eval(newOpts.script)
              .provideCustomLayer(layer(newOpts))
          }
      }
      .foldM(
        {
          case ConfigLoadFailure =>
            ZIO.succeed(ExitCode.failure)
          case error =>
            log
              .throwable("An unhandled error occurred", error)
              .as(ExitCode.failure)
        },
        _ => ZIO.succeed(ExitCode.success)
      )
      .provideCustomLayer(logging)
}
