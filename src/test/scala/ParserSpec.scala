package dynamite

import org.scalatest._
import Ast._

class ParserSpec extends FlatSpec with Matchers {

  "parser" should "parse wildcard fields" in {
    val result = Parser("select * from collection").get.value
    result should be(Select(All, "collection"))
  }

  "parser" should "parse a single field" in {
    val result = Parser("select id from collection").get.value
    result should be(Select(Fields(Seq("id")), "collection"))
  }

  it should "allow multiple fields" in {
    val result = Parser("select id,name from collection").get.value
    result should be(Select(Fields(Seq("id", "name")), "collection"))
  }

  it should "tolerate whitespace between fields" in {
    val result = Parser("select id,    name from collection").get.value
    result should be(Select(Fields(Seq("id", "name")), "collection"))
  }

  it should "tolerate whitespace before from" in {
    val result = Parser("select id, name     from collection").get.value
    result should be(Select(Fields(Seq("id", "name")), "collection"))
  }

  it should "tolerate whitespace before table name" in {
    val result = Parser("select id, name from          collection").get.value
    result should be(Select(Fields(Seq("id", "name")), "collection"))
  }

  it should "support filtering by a hash key" in {
    val result = Parser("select id, name from collection where id = 'user-id-1'").get.value
    result should be(Select(
      Fields(Seq("id", "name")),
      "collection",
      Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
    ))
  }

  it should "support filtering by a hash and sort key" in {
    val result = Parser("select id, name from collection where userId = 1 and id = 'user-id-1'").get.value
    result should be(Select(
      Fields(Seq("id", "name")),
      "collection",
      Option(
        PrimaryKey(
          Key("userId", IntValue(1)),
          Some(Key("id", StringValue("user-id-1")))
        )
      )
    ))
  }

  it should "support double-quoted string as well" in {
    val result = Parser("select id, name from collection where id = \"user-id-1\"").get.value
    result should be(Select(
      Fields(Seq("id", "name")),
      "collection",
      Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
    ))
  }

  it should "tolerate spaces around limit" in {
    val result = Parser("select id, name from collection    limit 10").get.value
    result should be(Select(
      Fields(Seq("id", "name")),
      "collection",
      limit = Some(10)
    ))
  }

  it should "allow selecting ascending order" in {
    val result = Parser("select id, name from collection asc limit 10").get.value
    result should be(Select(
      Fields(Seq("id", "name")),
      "collection",
      None,
      Some(Ascending),
      Some(10)
    ))
  }

  it should "allow selecting descending order" in {
    val result = Parser("select id, name from collection desc limit 10").get.value
    result should be(Select(
      Fields(Seq("id", "name")),
      "collection",
      None,
      Some(Descending),
      Some(10)
    ))
  }

  it should "allow updating a field" in {
    val result = Parser("update users set name = 'dave' where id = 'user-id-1'").get.value
    result should be(Update(
      "users",
      Seq("name" -> StringValue("dave")),
      PrimaryKey(Key("id", StringValue("user-id-1")), None)
    ))
  }

  it should "allow updating a field with hash and sort key" in {
    val result = Parser(
      "update playlists set name = 'Chillax' where userId = 'user-id-1' and id = 1"
    ).get.value
    result should be(Update(
      "playlists",
      Seq("name" -> StringValue("Chillax")),
      PrimaryKey(
        Key("userId", StringValue("user-id-1")),
        Some(Key("id", IntValue(1)))
      )
    ))
  }
}

