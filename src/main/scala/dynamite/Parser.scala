package dynamite

import fastparse._, NoWhitespace._
import Ast._
import dynamite.Ast.Projection.{Aggregate, FieldSelector}

//TODO: case insensitive keyword
object Parser {

  def keyword[A: P](value: String) = P(IgnoreCase(value))

  def opt[A, B: P](p: => P[A]): P[Option[A]] = (spaces ~ p).?

  def commaSeparated[A, B: P](parser: => P[A]) =
    parser.rep(1, sep = space.rep ~ "," ~/ space.rep)

  def space[A: P] = P(" " | "\n")

  def spaces[A: P] = P(space.rep(1))

  //TODO: use .opaque for this
  def character[A: P] = P(CharIn("a-z", "A-Z", "0-9") | "-" | "_")

  //TODO: support hyphens in fields?
  def ident[A: P] = P(character.rep(1).!)

  def str[A: P](delim: Char) =
    P(s"$delim" ~ CharsWhile(!s"$delim".contains(_)).rep.! ~ s"$delim")

  def setField[A, B: P](value: => P[A]) =
    P(ident ~ space.rep ~ "=" ~ space.rep ~ value)

  def string[A: P] = P(str('"') | str('\''))

  def digits[A: P] = P(CharsWhileIn("0-9"))

  def integer[A: P] = P("-".? ~ ("0" | CharIn("1-9") ~ digits.?)).!

  def float[A: P] = P("-".? ~ integer.? ~ "." ~ integer).!
  def boolValue[A: P]: P[BoolValue] = P(
    P("true").map(_ => BoolValue(true)) |
      P("false").map(_ => BoolValue(false))
  )
  def stringValue[A: P] = P(string.map(StringValue))
  def floatValue[A: P] = P(float.map(FloatValue))
  def integerValue[A: P] = P(integer.map(IntValue))
  def numberValue[A: P] = P(floatValue | integerValue)

  // keys support strings, number, and binary (TODO: support 'binary' input?)
  def keyValue[A: P]: P[KeyValue] = P(stringValue | numberValue)

  //TODO: distinguish set/list in some operations?
  def listValue[A: P]: P[ListValue] =
    P(
      "[" ~/ space.rep ~ commaSeparated(value).? ~ space.rep ~ "]"
    ).map(value => ListValue(value.getOrElse(Seq.empty)))

  def objectValue[A: P]: P[ObjectValue] =
    P(
      "{" ~/ space.rep ~ commaSeparated(
        string ~ space.rep ~ ":" ~ space.rep ~ value
      ) ~ space.rep ~ "}"
    ).map(values => ObjectValue(values))

  def value[A: P] = P(keyValue | listValue | objectValue | boolValue)

  //TODO: fail parse on invalid numbers?
  def limit[A: P] = P(keyword("limit") ~/ spaces ~ integer.map(_.toInt))

  def key[A: P] = P(setField(keyValue).map(Key.tupled))

  def primaryKey[A: P] =
    P(
      keyword("where") ~/ spaces ~ key
        ~ (spaces ~ keyword("and") ~/ spaces ~ key).?
    ).map {
      case (hash, sortKey) =>
        PrimaryKey(hash, sortKey)
    }

  def direction[A: P]: P[Direction] = P(
    P(keyword("asc")).map(_ => Ascending) |
      P(keyword("desc")).map(_ => Descending)
  )

  def orderBy[A: P] =
    P(
      (keyword("order") ~/ spaces ~ keyword("by") ~ spaces ~ ident) ~ opt(
        direction
      )
    ).map {
      case (field, direction) =>
        OrderBy(field, direction)
    }

  def from[A: P] = P(keyword("from") ~ spaces ~ ident)

  def field[A: P] = P(ident).map(FieldSelector.Field)

  def aggregateFunction[A: P] = StringInIgnoreCase("count").!.map(_.toLowerCase)

  def aggregate[A: P]: P[Aggregate] =
    P(
      aggregateFunction ~ spaces.? ~ "(" ~/ spaces.? ~ allFields ~ spaces.? ~ ")"
    ).flatMap {
      case ("count", _) => Pass.map(_ => Aggregate.Count)
      case _            =>
        //TODO: custom exception with all valid aggregates?
        Fail.opaque("aggregate function")
    }

  def allFields[A: P] = P("*".!.map(_ => FieldSelector.All))

  def fieldSelector[A: P]: P[FieldSelector] = P(
    allFields | field
  )

  def projection[A: P]: P[Projection] = P(
    // TODO: support select sum(field), a, etc.
    aggregate | fieldSelector
  )

  def projections[A: P]: P[Seq[Projection]] = P(
    commaSeparated(projection)
  )

  def useIndex[A: P] =
    keyword("use") ~ spaces ~ keyword("index") ~ spaces ~ ident

  def select[A: P] =
    P(
      keyword("select") ~/ spaces ~
        projections ~ spaces ~
        from ~
        opt(primaryKey) ~
        opt(orderBy) ~
        opt(limit) ~
        opt(useIndex)
    ).map(Select.tupled)

  def update[A: P] =
    P(
      keyword("update") ~/ spaces ~ ident ~ spaces ~
        keyword("set") ~ spaces ~ commaSeparated(setField(value)) ~ spaces ~
        primaryKey
    ).map(Update.tupled)

  def delete[A: P] =
    P(keyword("delete") ~/ spaces ~ from ~ spaces ~ primaryKey)
      .map(Delete.tupled)

  def table[A: P] = P(ident)

  def insert[A: P] =
    P(
      keyword("insert") ~/ spaces ~
        keyword("into") ~ spaces ~ table ~ space.rep ~
        // it would be nice to have column names be optional, but there is no
        // stable order of a dynamo schema. Some convention could be introduced, but it
        // might surprising. Optional feature?
        "(" ~/ space.rep ~ commaSeparated(ident) ~ space.rep ~ ")" ~ space.rep ~
        keyword("values") ~ space.rep ~ "(" ~ space.rep ~ commaSeparated(value) ~ ")"
    ).map {
      case (table, keys, values) =>
        val pairs = keys.zip(values)
        Insert(table, pairs)
    }

  def describeTable[A: P] =
    P(keyword("describe") ~ spaces ~ keyword("table") ~ spaces ~ ident)
      .map(DescribeTable)

  def showTables[A: P] =
    P(keyword("show") ~ spaces ~ keyword("tables")).map(_ => ShowTables)

  def format[A: P]: P[Ast.Format] = P(
    keyword("tabular").map(_ => Ast.Format.Tabular) |
      keyword("json").map(_ => Ast.Format.Json)
  )

  def setFormat[A: P] = P(keyword("format") ~/ spaces ~ format).map(SetFormat)

  def showFormat[A: P] =
    P(keyword("show") ~ spaces ~ keyword("format"))
      .map(_ => ShowFormat)

  def createTable[A: P] =
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

  def query[A: P]: P[Command] =
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
