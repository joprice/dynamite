package dynamite

import java.util.{ Iterator => JIterator }

import com.amazonaws.services.dynamodbv2.document.Page

import scala.util.Try

class RecoveringIterator[A, B](
    original: JIterator[Page[A, B]]
) extends Iterator[Try[JIterator[A]]] {
  private[this] var failed = false

  def next() = {
    val result = Try(original.next.iterator())
    failed = result.isFailure
    result
  }

  def hasNext = !failed && original.hasNext
}
