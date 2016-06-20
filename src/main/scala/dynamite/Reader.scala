package dynamite

import jline.console.ConsoleReader

trait Reader {
  def disableEcho(): Unit

  def enableEcho(): Unit

  def clearPrompt(): Unit

  def resetPrompt(): Unit

  def readCharacter(): Int

  def readLine(): String

  def setPrompt(prompt: String): Unit
}

class JLineReader(reader: ConsoleReader) extends Reader {
  def disableEcho(): Unit = reader.setEchoCharacter(new Character(0))

  def enableEcho(): Unit = reader.setEchoCharacter(null)

  def clearPrompt(): Unit = reader.setPrompt("")

  def resetPrompt(): Unit = Repl.resetPrompt(reader)

  def readCharacter(): Int = reader.readCharacter()

  def readLine(): String = reader.readLine()

  def setPrompt(prompt: String): Unit = reader.setPrompt(">")
}
