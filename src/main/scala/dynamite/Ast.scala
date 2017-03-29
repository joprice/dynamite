package dynamite

object Ast {

  sealed abstract class Command extends Product with Serializable

  sealed abstract class ReplCommand extends Command

  case object ShowFormat extends ReplCommand

  final case class SetFormat(format: Format) extends ReplCommand

  sealed abstract class Format extends Product with Serializable {
    override def toString: String = this match {
      case Format.Json => "json"
      case Format.Tabular => "tabular"
    }
  }

  object Format {
    case object Json extends Format
    case object Tabular extends Format
  }

  sealed abstract class Query extends Command

  final case class Select(
    projection: Seq[Projection],
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

  final case class DescribeTable(table: String) extends Query

  sealed abstract class Direction extends Product with Serializable
  case object Ascending extends Direction
  case object Descending extends Direction

  sealed abstract class Projection extends Product with Serializable

  object Projection {
    sealed abstract class Aggregate(val name: String) extends Projection

    object Aggregate {
      //TODO: for now, field name is same as function, should add aliases
      //TODO: for now, not accepting a projection. In the future arbitrary should be selected, which would sum non-null values
      case object Count extends Aggregate("count")
    }

    sealed abstract class FieldSelector extends Projection

    object FieldSelector {
      final case class Field(name: String) extends FieldSelector
      case object All extends FieldSelector
    }
  }

  sealed abstract class Value extends Product with Serializable

  sealed abstract class KeyValue extends Value

  final case class StringValue(value: String) extends KeyValue

  /**
   * NOTE: The constructors of IntValue and FloatValue keep their original string
   * original representation for debugging purposes.
   */
  sealed abstract class NumberValue(val value: Number) extends KeyValue
  final case class IntValue(repr: String) extends NumberValue(BigInt(repr))
  final case class FloatValue(repr: String) extends NumberValue(BigDecimal(repr))

  final case class ListValue(values: Seq[Value]) extends Value

  final case class ObjectValue(values: Seq[(String, Value)]) extends Value

  final case class Key(field: String, value: KeyValue)

  final case class PrimaryKey(hash: Key, range: Option[Key])
}

