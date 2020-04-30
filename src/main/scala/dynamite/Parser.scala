package dynamite

import fastparse._, NoWhitespace._
import Ast._
import dynamite.Ast.Projection.{Aggregate, FieldSelector}

//TODO: case insensitive keyword
object Parser {

  def keyword[_: P](value: String) = P(IgnoreCase(value))

  def opt[A, _: P](p: => P[A]): P[Option[A]] = (spaces ~ p).?

  def commaSeparated[A, _: P](parser: => P[A]) =
    parser.rep(1, sep = space.rep ~ "," ~/ space.rep)

  def space[_: P] = P(" " | "\n")

  def spaces[_: P] = P(space.rep(1))

  //TODO: use .opaque for this
  def character[_: P] = P(CharIn("a-z", "A-Z", "0-9") | "-" | "_")

  //TODO: support hyphens in fields?
  def ident[_: P] = P(character.rep(1).!)

  def str[_: P](delim: Char) =
    P(s"$delim" ~ CharsWhile(!s"$delim".contains(_)).rep.! ~ s"$delim")

  def setField[A, _: P](value: => P[A]) =
    P(ident ~ space.rep ~ "=" ~ space.rep ~ value)

  def string[_: P] = P(str('"') | str('\''))

  def digits[_: P] = P(CharsWhileIn("0-9"))

  def integer[_: P] = P("-".? ~ ("0" | CharIn("1-9") ~ digits.?)).!

  def float[_: P] = P("-".? ~ integer.? ~ "." ~ integer).!
  def boolValue[_: P]: P[BoolValue] = P(
    P("true").map(_ => BoolValue(true)) |
      P("false").map(_ => BoolValue(false))
  )
  def stringValue[_: P] = P(string.map(StringValue))
  def floatValue[_: P] = P(float.map(FloatValue))
  def integerValue[_: P] = P(integer.map(IntValue))
  def numberValue[_: P] = P(floatValue | integerValue)

  // keys support strings, number, and binary (TODO: support 'binary' input?)
  def keyValue[_: P]: P[KeyValue] = P(stringValue | numberValue)

  //TODO: distinguish set/list in some operations?
  def listValue[_: P]: P[ListValue] =
    P(
      "[" ~/ space.rep ~ commaSeparated(value).? ~ space.rep ~ "]"
    ).map(value => ListValue(value.getOrElse(Seq.empty)))

  def objectValue[_: P]: P[ObjectValue] =
    P(
      "{" ~/ space.rep ~ commaSeparated(
        string ~ space.rep ~ ":" ~ space.rep ~ value
      ) ~ space.rep ~ "}"
    ).map(values => ObjectValue(values))

  def value[_: P] = P(keyValue | listValue | objectValue | boolValue)

  //TODO: fail parse on invalid numbers?
  def limit[_: P] = P(keyword("limit") ~/ spaces ~ integer.map(_.toInt))

  def key[_: P] = P(setField(keyValue).map(Key.tupled))

  def primaryKey[_: P] =
    P(
      keyword("where") ~/ spaces ~ key
        ~ (spaces ~ keyword("and") ~/ spaces ~ key).?
    ).map {
      case (hash, sortKey) =>
        PrimaryKey(hash, sortKey)
    }

  def direction[_: P]: P[Direction] = P(
    P(keyword("asc")).map(_ => Ascending) |
      P(keyword("desc")).map(_ => Descending)
  )

  def orderBy[_: P] =
    P(
      (keyword("order") ~/ spaces ~ keyword("by") ~ spaces ~ ident) ~ opt(
        direction
      )
    ).map {
      case (field, direction) =>
        OrderBy(field, direction)
    }

  def from[_: P] = P(keyword("from") ~ spaces ~ ident)

  def field[_: P] = P(ident).map(FieldSelector.Field)

  def aggregateFunction[_: P] = StringInIgnoreCase("count").!.map(_.toLowerCase)

  def aggregate[_: P]: P[Aggregate] =
    P(
      aggregateFunction ~ spaces.? ~ "(" ~/ spaces.? ~ allFields ~ spaces.? ~ ")"
    ).flatMap {
      case ("count", _) => Pass.map(_ => Aggregate.Count)
      case _            =>
        //TODO: custom exception with all valid aggregates?
        Fail.opaque("aggregate function")
    }

  def allFields[_: P] = P("*".!.map(_ => FieldSelector.All))

  def fieldSelector[_: P]: P[FieldSelector] = P(
    allFields | field
  )

  def projection[_: P]: P[Projection] = P(
    // TODO: support select sum(field), a, etc.
    aggregate | fieldSelector
  )

  def projections[_: P]: P[Seq[Projection]] = P(
    commaSeparated(projection)
  )

  def useIndex[_: P] =
    keyword("use") ~ spaces ~ keyword("index") ~ spaces ~ ident

  def select[_: P] =
    P(
      keyword("select") ~/ spaces ~
        projections ~ spaces ~
        from ~
        opt(primaryKey) ~
        opt(orderBy) ~
        opt(limit) ~
        opt(useIndex)
    ).map(Select.tupled)

  def update[_: P] =
    P(
      keyword("update") ~/ spaces ~ ident ~ spaces ~
        keyword("set") ~ spaces ~ commaSeparated(setField(value)) ~ spaces ~
        primaryKey
    ).map(Update.tupled)

  def delete[_: P] =
    P(keyword("delete") ~/ spaces ~ from ~ spaces ~ primaryKey)
      .map(Delete.tupled)

  def table[_: P] = P(ident)

  def insert[_: P] =
    P(
      keyword("insert") ~/ spaces ~
        keyword("into") ~ spaces ~ table ~ space.rep ~
        // it would be nice to have column names be optional, but there is no
        // stable order of a dynamo schema. Some convention could be introduced, but it
        // might surprising. Optional feature?
        "(" ~/ space.rep ~ commaSeparated(ident) ~ space.rep ~ ")" ~ spaces ~
        keyword("values") ~ spaces ~ "(" ~ space.rep ~ commaSeparated(value) ~ ")"
    ).map {
      case (table, keys, values) =>
        val pairs = keys.zip(values)
        Insert(table, pairs)
    }

  def describeTable[_: P] =
    P(keyword("describe") ~ spaces ~ keyword("table") ~ spaces ~ ident)
      .map(DescribeTable)

  def showTables[_: P] =
    P(keyword("show") ~ spaces ~ keyword("tables")).map(_ => ShowTables)

  def format[_: P]: P[Ast.Format] = P(
    keyword("tabular").map(_ => Ast.Format.Tabular) |
      keyword("json").map(_ => Ast.Format.Json)
  )

  def setFormat[_: P] = P(keyword("format") ~/ spaces ~ format).map(SetFormat)

  def showFormat[_: P] =
    P(keyword("show") ~ spaces ~ keyword("format"))
      .map(_ => ShowFormat)

  def createTable[_: P] =
    P(
      keyword("create") ~/ spaces ~ keyword("table") ~
        (spaces ~ keyword("if") ~/ spaces ~ keyword("not") ~ spaces ~ keyword(
          "exists"
        )).!.?.map(_.isDefined) ~
        spaces ~ table ~ space.rep ~ "(" ~ ident ~ spaces ~ ident ~ space.rep ~ ")"
    ).map {
      case (ignoreExisting, table, hash, hashType) =>
        CreateTable(
          tableName = table,
          name = hash,
          typeName = hashType,
          ignoreExisting = ignoreExisting
        )
    }

  def query[_: P]: P[Command] =
    P(
      spaces.? ~ (
        update | select | delete | insert | createTable | showTables | describeTable | setFormat | showFormat
      ) ~ spaces.? ~ End
    )

  sealed abstract class ParseException(
      message: String,
      cause: Option[Throwable]
  ) extends Exception(message, cause.orNull)

  object ParseException {
    final case class ParseError(failure: Parsed.Failure)
        extends ParseException(
          // see notes on errors http://www.lihaoyi.com/fastparse/#Failures
          "Failed parsing query",
          Some(new Exception(s"${failure.index} ${failure.msg}"))
        )
    final case class UnAggregatedFieldsError(fields: Seq[String])
        extends ParseException(
          "Aggregates may not be used with unaggregated fields",
          None
        )
  }

  def validate(select: Select): Either[ParseException, Query] = {
    val (aggs, fields) = select.projection.foldLeft(
      (Seq.empty: Seq[Aggregate], Vector.empty[String])
    ) {
      case (state, FieldSelector.All) => state
      case ((aggs, fields), FieldSelector.Field(field)) =>
        (aggs, fields :+ field)
      case ((aggs, fields), count: Aggregate.Count.type) =>
        (aggs :+ count, fields)
    }
    if (aggs.nonEmpty && fields.nonEmpty) {
      Left(ParseException.UnAggregatedFieldsError(fields))
    } else {
      Right(select)
    }
  }

  def parse(input: String): Either[ParseException, Command] =
    // import explicitly as a workaround to this https://github.com/lihaoyi/fastparse/issues/34
    fastparse.parse(input, query(_)) match {
      case Parsed.Success(select: Select, _) => validate(select)
      case Parsed.Success(value, _)          => Right(value)
      case failure: Parsed.Failure           => Left(ParseException.ParseError(failure))
    }
}
