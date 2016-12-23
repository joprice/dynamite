package dynamite

import scala.util.Try

class RecoveringIterator[A, B](
    original: Iterator[Iterator[A]]
) extends Iterator[Try[Iterator[A]]] {
  private[this] var failed = false

  def next() = {
    val result = Try(original.next())
    failed = result.isFailure
    result
  }

  def hasNext = !failed && original.hasNext
}
