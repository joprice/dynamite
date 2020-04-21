package dynamite

import scala.util.Try

class RecoveringIterator[A](
    original: Iterator[A]
) extends Iterator[Try[A]] {
  private[this] var failed = false

  def next() = {
    val result = Try(original.next())
    failed = result.isFailure
    result
  }

  def hasNext = !failed && original.hasNext
}
