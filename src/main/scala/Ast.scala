package dynamite

import java.io.Serializable

//TODO: value type
object Ast {

  sealed abstract class Query extends Product with Serializable

  final case class Select(
    projection: Projection,
    from: String,
    where: Option[PrimaryKey] = None,
    direction: Option[Direction] = None,
    limit: Option[Int] = None
  ) extends Query

  final case class Update(
    table: String,
    fields: Seq[(String, Value)],
    key: PrimaryKey
  ) extends Query

  final case class Delete(
    table: String,
    key: PrimaryKey
  ) extends Query

  final case class Insert(
    table: String,
    values: Seq[Key]
  ) extends Query

  case object ShowTables extends Query

  sealed abstract class Direction extends Product with Serializable
  case object Ascending extends Direction
  case object Descending extends Direction

  sealed abstract class Projection extends Product with Serializable
  final case class Fields(fields: Seq[String]) extends Projection
  case object All extends Projection

  sealed abstract class Value extends Product with Serializable
  final case class StringValue(value: String) extends Value
  final case class IntValue(value: Int) extends Value

  final case class Ident(name: String)

  //TODO: multiple filters
  //  final case class Where(field: String, value: Value)

  final case class Key(field: String, value: Value)

  final case class PrimaryKey(hash: Key, range: Option[Key])
}

