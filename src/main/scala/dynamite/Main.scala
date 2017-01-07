package dynamite

object Main {

  def main(args: Array[String]): Unit = {
    Opts.parser.parse(args, Opts()) match {
      case Some(config) =>
        config.script match {
          case Some(script) => Script(config, script)
          case None =>
            if (System.console() == null) {
              Script(config)
            } else {
              Repl(config)
            }
        }
      case None => sys.exit(1)
    }
  }

}
