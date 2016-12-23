package dynamite

import java.io._
import java.nio.charset.StandardCharsets

class TestReader(line: String) extends Reader {
  private[this] val in = new BufferedReader(
    new InputStreamReader(
      new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8))
    )
  )

  def close() = in.close()

  def terminalWidth = 100
  def enableEcho(): Unit = ()
  def clearPrompt(): Unit = ()
  def resetPrompt(): Unit = ()
  def setPrompt(prompt: String): Unit = ()
  def disableEcho(): Unit = ()
  def readLine(): String = in.readLine()
  def readCharacter(): Int = ' '
}
