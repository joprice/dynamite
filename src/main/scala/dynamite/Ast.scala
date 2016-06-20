package dynamite

object Ast {

  sealed abstract class Query extends Product with Serializable

  final case class Select(
    projection: Projection,
    from: String,
    where: Option[PrimaryKey] = None,
    direction: Option[Direction] = None,
    limit: Option[Int] = None,
    useIndex: Option[String] = None
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
    values: Seq[(String, Value)]
  ) extends Query

  case object ShowTables extends Query

  case class DescribeTable(table: String) extends Query

  sealed abstract class Direction extends Product with Serializable
  case object Ascending extends Direction
  case object Descending extends Direction

  sealed abstract class Projection extends Product with Serializable
  final case class Fields(fields: Seq[String]) extends Projection
  case object All extends Projection

  sealed abstract class Value extends Product with Serializable

  sealed abstract class KeyValue extends Value

  final case class StringValue(value: String) extends KeyValue

  /**
   * NOTE: The constructors of IntValue and FloatValue keep their original string
   * original representatio for debugging purposes.
   */
  sealed abstract class NumberValue(val value: Number) extends KeyValue
  final case class IntValue(repr: String) extends NumberValue(BigInt(repr))
  final case class FloatValue(repr: String) extends NumberValue(BigDecimal(repr))

  final case class ListValue(values: Seq[Value]) extends Value

  final case class Ident(name: String)

  final case class Key(field: String, value: KeyValue)

  final case class PrimaryKey(hash: Key, range: Option[Key])
}

