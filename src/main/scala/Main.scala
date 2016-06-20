package dynamite

object Main {

  def main(args: Array[String]): Unit = {
    Opts.parser.parse(args, Opts()) match {
      case Some(config) =>
        if (System.console() == null) {
          Script(config)
        } else {
          Repl(config)
        }
      case None =>
    }
  }

}
