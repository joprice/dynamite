package dynamite

import fastparse.all._
import Ast._
import dynamite.Ast.Projection.{ Aggregate, FieldSelector }

//TODO: case insensitive keyword
object Parser {

  def opt[A](p: Parser[A]): Parser[Option[A]] = (spaces ~ p).?

  def commaSeparated[A](parser: Parser[A]) = parser.rep(1, sep = "," ~/ space.rep)

  val space = P(" " | "\n")

  val spaces = P(space.rep(1))

  //TODO: use .opaque for this
  val character = P(CharIn('a' to 'z', 'A' to 'Z', '0' to '9', Seq('-')))

  //TODO: support hyphens in fields?
  val ident = P(character.rep(1).!)

  def str(delim: Char) =
    P(s"$delim" ~ CharsWhile(!s"$delim".contains(_)).rep.! ~ s"$delim")

  def setField[A](value: Parser[A]) = P(ident ~ space.rep ~ "=" ~ space.rep ~ value)

  val string = P(str('"') | str('\''))

  val integer = P("-".? ~ {
    val num = P(CharIn('0' to '9'))
    val nonZeroNum = P(CharIn('1' to '9'))
    P(nonZeroNum ~ num.rep | "0").!
  }).!

  val float = P("-".? ~ integer.? ~ "." ~ integer).!

  val stringValue = P(string.map(StringValue(_)))
  val floatValue = P(float.map(FloatValue(_)))
  val integerValue = P(integer.map(IntValue(_)))
  val numberValue = P(floatValue | integerValue)

  // keys support strings, number, and binary (TODO: support 'binary' input?)
  val keyValue: Parser[KeyValue] = P(stringValue | numberValue)

  //TODO: distinguish set/list in some operations?
  val listValue: Parser[ListValue] = P(
    "[" ~/ space.rep ~ commaSeparated(value).? ~ space.rep ~ "]"
  ).map(value => ListValue(value.getOrElse(Seq.empty)))

  val value = P(keyValue | listValue)

  //TODO: fail parse on invalid numbers?
  val limit = P("limit" ~/ spaces ~ integer.map(_.toInt))

  val key = P(setField(keyValue).map(Key.tupled))

  val primaryKey = P(
    "where" ~/ spaces ~ key
      ~ (spaces ~ "and" ~/ spaces ~ key).?
  ).map {
      case (hash, sortKey) =>
        PrimaryKey(hash, sortKey)
    }

  val direction: Parser[Direction] = P(
    P("asc").map(_ => Ascending) |
      P("desc").map(_ => Descending)
  )

  val from = P("from" ~ spaces ~ ident)

  val field = P(ident).map(FieldSelector.Field)

  val aggregateFunction = P("count".!)

  val aggregate: Parser[Aggregate] = P(
    aggregateFunction ~ spaces.? ~ "(" ~ spaces.? ~ allFields ~ spaces.? ~ ")"
  ).map {
      case ("count", _) => Aggregate.Count
      case (other, _) =>
        //TODO: custom exception with all valid aggregates
        throw new Exception(s"$other is not a valid aggregate function")
    }

  val allFields = P("*".!.map(_ => FieldSelector.All))

  val fieldSelector: Parser[FieldSelector] = P(
    allFields | field
  )

  val projection: Parser[Projection] = P(
    // TODO: support select sum(field), a, etc.
    aggregate | fieldSelector
  )

  val projections: Parser[Seq[Projection]] = P(
    commaSeparated(projection)
  )

  val useIndex = "use" ~ spaces ~ "index" ~ spaces ~ ident

  val select = P(
    "select" ~/ spaces ~
      projections ~ spaces ~
      from ~
      opt(primaryKey) ~
      opt(direction) ~
      opt(limit) ~
      opt(useIndex)
  ).map(Select.tupled)

  val update = P(
    "update" ~/ spaces ~ ident ~ spaces ~
      "set" ~ spaces ~ commaSeparated(setField(value)) ~ spaces ~
      primaryKey
  )
    .map(Update.tupled)

  val delete = P("delete" ~/ spaces ~ from ~ spaces ~ primaryKey)
    .map(Delete.tupled)

  val insert = P(
    "insert" ~/ spaces ~
      "into" ~ spaces ~ ident ~ spaces ~
      // it would be nice to have column names be optional, but there is no
      // stable order of a dynamo schema. Some convention could be introduced, but it
      // might surprising. Optional feature?
      "(" ~/ space.rep ~ commaSeparated(ident) ~ space.rep ~ ")" ~ spaces ~
      "values" ~ spaces ~ "(" ~ space.rep ~ commaSeparated(value) ~ ")"
  ).map {
      case (table, keys, values) =>
        val pairs = keys.zip(values)
        Insert(table, pairs)
    }

  val describeTable = P("describe" ~ spaces ~ "table" ~ spaces ~ ident).map(DescribeTable(_))

  val showTables = P("show" ~ spaces ~ "tables").map(_ => ShowTables)

  val query = P(spaces.? ~ (
    update | select | delete | insert | showTables | describeTable
  ) ~ spaces.? ~ End)

  sealed abstract class ParseException(message: String, cause: Option[Throwable])
    extends Exception(message, cause.orNull)
  final case class ParseError(failure: Parsed.Failure) extends ParseException(
    "Failed parsing query", Some(fastparse.core.ParseError(failure))
  )
  final case class UnAggregatedFieldsError(fields: Seq[String]) extends ParseException(
    "Aggregates may not be used with unaggregated fields", None
  )

  def validate(query: Query): Either[ParseException, Query] = query match {
    case select: Select =>
      val (aggs, fields) = select.projection.foldLeft((Seq.empty: Seq[Aggregate], Vector.empty[String])) {
        case (state, FieldSelector.All) => state
        case ((aggs, fields), FieldSelector.Field(field)) => (aggs, fields :+ field)
        case ((aggs, fields), count: Aggregate.Count.type) => (aggs :+ count, fields)
      }
      if (aggs.nonEmpty && fields.nonEmpty) {
        Left(UnAggregatedFieldsError(fields))
      } else {
        Right(select)
      }
    case _ => Right(query)
  }

  def apply(input: String): Either[ParseException, Query] = {
    // import explicitly as a workaround to this https://github.com/lihaoyi/fastparse/issues/34
    import fastparse.core.Parsed.{ Failure, Success }
    query.parse(input) match {
      case Success(select: Select, _) => validate(select)
      case Success(value, _) => Right(value)
      case failure: Failure[_, _] => Left(ParseError(failure))
    }
  }
}
