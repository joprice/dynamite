package dynamite

import dynamite.Dynamo.DynamoObject
import zio.stream.ZStream

sealed abstract class Paging[+A] extends Product with Serializable

object Paging {

  final case class Page[A](data: ZStream[Any, Nothing, A]) extends Paging[A]

  case object EOF extends Paging[Nothing]
}

sealed abstract class PageType extends Product with Serializable {
  val hasMore: Boolean
}

object PageType {
  final case class TablePaging(
      select: Ast.Select,
      data: List[DynamoObject],
      hasMore: Boolean
  ) extends PageType

  final case class TableNamePaging(names: Seq[String], hasMore: Boolean)
      extends PageType
}
