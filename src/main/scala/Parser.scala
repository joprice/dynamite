package dynamite

import fastparse.all._

import Ast._

//TODO: case insensitive keyword
object Parser {

  val space = P(" ")

  //TODO: support new lines?
  val spaces = P(space.rep(1))

  def opt[A](p: Parser[A]): Parser[Option[A]] = (spaces ~ p).?

  //TODO: support hyphens in fields?
  val ident = P(CharIn('a' to 'z', 'A' to 'Z', '0' to '9', Seq('-')).rep.!)

  val fields = P(ident.rep(1, sep = "," ~ space.rep))

  val select = P("select" ~/ spaces ~ ("*".!.map(_ => All) | fields.map(Fields(_))))

  val from = P("from" ~ spaces ~ ident)

  def str(delim: Char) =
    P(s"$delim" ~ CharsWhile(!s"$delim".contains(_)).rep.! ~ s"$delim")

  val string = P(str('"') | str('\''))

  val integer = {
    val num = P(CharIn('0' to '9'))
    val nonZeroNum = P(CharIn('1' to '9'))
    P(nonZeroNum ~ num.rep | "0").!.map(_.toInt)
  }

  val value = P(string.map(StringValue(_)) | integer.map(IntValue(_)))

  val setField = P(ident ~ space.rep ~ "=" ~ space.rep ~ value)

  //TODO: fail parse on invalid numbers?
  val limit = {
    "limit" ~/ spaces ~ integer
  }

  val direction: Parser[Direction] = P(
    P("asc").map(_ => Ascending) |
      P("desc").map(_ => Descending)
  )

  val key = P(setField.map((Key.apply _).tupled))

  val primaryKey = P(
    "where" ~/ spaces ~ key
      ~ (spaces ~ "and" ~/ spaces ~ key).?
  ).map {
      case (hash, sortKey) =>
        PrimaryKey(hash, sortKey)
    }

  val query = P(
    select ~ spaces ~
      from ~
      opt(primaryKey) ~
      opt(direction) ~
      opt(limit)
  ).map((Select.apply _).tupled)

  val update = P(
    "update" ~/ spaces ~ ident ~ spaces ~
      "set" ~ spaces ~ setField.rep(1, sep = "," ~/ spaces) ~ spaces ~
      primaryKey
  )
    .map((Update.apply _).tupled)

  val delete = P("delete" ~/ spaces ~ from ~ spaces ~ primaryKey)
    .map((Delete.apply _).tupled)

  def apply(input: String): Parsed[Query] = (
    (update | query | delete) ~ spaces.? ~ End
  ).parse(input)

}
