package dynamite

import fastparse.all._

import Ast._

//TODO: case insensitive keyword
object Parser {

  def opt[A](p: Parser[A]): Parser[Option[A]] = (spaces ~ p).?

  def commaSeparated[A](parser: Parser[A]) = parser.rep(1, sep = "," ~/ space.rep)

  val space = P(" ")

  //TODO: support new lines?
  val spaces = P(space.rep(1))

  //TODO: support hyphens in fields?
  val ident = P(CharIn('a' to 'z', 'A' to 'Z', '0' to '9', Seq('-')).rep.!)

  def str(delim: Char) =
    P(s"$delim" ~ CharsWhile(!s"$delim".contains(_)).rep.! ~ s"$delim")

  val string = P(str('"') | str('\''))

  val integer = {
    val num = P(CharIn('0' to '9'))
    val nonZeroNum = P(CharIn('1' to '9'))
    P(nonZeroNum ~ num.rep | "0").!.map(_.toInt)
  }

  def setField[A](value: Parser[A]) = P(ident ~ space.rep ~ "=" ~ space.rep ~ value)

  val keyValue: Parser[KeyValue] =
    P(string.map(StringValue(_)) | integer.map(IntValue(_)))

  val listValue: Parser[ListValue] = P(
    "[" ~/ space.rep ~ commaSeparated(value).? ~ space.rep ~ "]"
  ).map(value => ListValue(value.getOrElse(Seq.empty)))

  val value = P(keyValue | listValue)

  //TODO: fail parse on invalid numbers?
  val limit = P("limit" ~/ spaces ~ integer)

  val key = P(setField(keyValue).map((Key.apply _).tupled))

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

  val fields = P(commaSeparated(ident))

  val projection = P(
    "select" ~/ spaces ~ ("*".!.map(_ => All) | fields.map(Fields(_)))
  )

  val select = P(
    projection ~ spaces ~
      from ~
      opt(primaryKey) ~
      opt(direction) ~
      opt(limit)
  ).map((Select.apply _).tupled)

  val update = P(
    "update" ~/ spaces ~ ident ~ spaces ~
      "set" ~ spaces ~ commaSeparated(setField(value)) ~ spaces ~
      primaryKey
  )
    .map((Update.apply _).tupled)

  val delete = P("delete" ~/ spaces ~ from ~ spaces ~ primaryKey)
    .map((Delete.apply _).tupled)

  val insert = P(
    "insert" ~/ spaces ~
      "into" ~ spaces ~ ident ~ spaces ~
      "(" ~/ space.rep ~ commaSeparated(ident) ~ space.rep ~ ")" ~ spaces ~
      "values" ~ spaces ~ "(" ~ space.rep ~ commaSeparated(value) ~ ")"
  ).map {
      case (table, keys, values) =>
        val pairs = keys.zip(values)
        Insert(table, pairs)
    }

  val showTables = P("show" ~ spaces ~ "tables").map(_ => ShowTables)

  val query = P((
    update | select | delete | insert | showTables
  ) ~ spaces.? ~ End)

  def apply(input: String): Parsed[Query] = query.parse(input)

}
