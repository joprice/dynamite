package dynamite

import com.fasterxml.jackson.databind.JsonNode

sealed abstract class Paging[+A] extends Product with Serializable

object Paging {
  def fromIterator[A](it: Iterator[A]): Paging[A] = {
    if (it.hasNext) {
      val next = Lazy(it.next())
      Page(next, Lazy {
        next()
        fromIterator(it)
      })
    } else EOF
  }

  final case class Page[A](data: Lazy[A], next: Lazy[Paging[A]]) extends Paging[A]

  case object EOF extends Paging[Nothing]
}

sealed abstract class PageType extends Product with Serializable

object PageType {
  final case class TablePaging(select: Ast.Select, data: List[JsonNode]) extends PageType
  final case class TableNamePaging(names: Seq[String]) extends PageType
}
