package dynamite

import java.util.stream.Collectors

object Dynamite {

  def main(args: Array[String]): Unit = {
    Opts.parse(args) match {
      case Some(config) =>
        config.script match {
          case Some(script) => Script(config, script)
          case None =>
            if (System.console() == null) {
              val input = Console.in.lines().collect(Collectors.joining("\n"))
              Script(config, input)
            } else {
              Repl(config)
            }
        }
      case None => sys.exit(1)
    }
  }

}
