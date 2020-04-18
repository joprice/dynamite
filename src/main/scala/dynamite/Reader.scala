package dynamite

import fansi.{ Bold, Str }
import jline.console.ConsoleReader

trait Reader {

  def resetPagination() = {
    enableEcho()
    resetPrompt()
  }

  def terminalWidth: Int

  def disableEcho(): Unit

  def enableEcho(): Unit

  def clearPrompt(): Unit

  def resetPrompt(): Unit

  def readCharacter(): Int

  def readLine(): String

  def setPrompt(prompt: String): Unit

}

class JLineReader(reader: ConsoleReader) extends Reader {
  def disableEcho(): Unit = reader.setEchoCharacter(Character.valueOf(0))

  def enableEcho(): Unit = reader.setEchoCharacter(null)

  def clearPrompt(): Unit = reader.setPrompt("")

  def resetPrompt(): Unit = reader.setPrompt(Bold.On(Str("dql> ")).render)

  def readCharacter(): Int = reader.readCharacter()

  def readLine(): String = reader.readLine()

  def setPrompt(prompt: String): Unit = reader.setPrompt(prompt)

  def terminalWidth: Int = reader.getTerminal.getWidth

}
