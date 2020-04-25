package dynamite

import dynamite.Dynamo.DynamoObject
import zio.stream.ZStream

sealed abstract class Paging[+A] extends Product with Serializable

object Paging {

  final case class Page[A](data: ZStream[Any, Nothing, A]) extends Paging[A]

  case object EOF extends Paging[Nothing]
}

sealed abstract class PageType extends Product with Serializable

object PageType {
  final case class TablePaging(
      select: Ast.Select,
      data: List[DynamoObject]
  ) extends PageType
  final case class TableNamePaging(names: Seq[String]) extends PageType
}
