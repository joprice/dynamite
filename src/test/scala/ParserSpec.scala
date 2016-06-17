package dynamite

import org.scalatest._
import Ast._

class ParserSpec extends FlatSpec with Matchers {

  def validate(query: String, result: Query) = {
    val result = Parser(query).get.value
    result should be(result)
  }

  "parser" should "parse wildcard fields" in {
    validate("select * from collection", Select(All, "collection"))
  }

  "parser" should "allow dashes in ident" in {
    validate("select * from collection-legacy", Select(All, "collection-legacy"))
  }

  "parser" should "parse a single field" in {
    validate("select id from collection", Select(Fields(Seq("id")), "collection"))
  }

  it should "allow multiple fields" in {
    validate(
      "select id,name from collection",
      Select(Fields(Seq("id", "name")), "collection")
    )
  }

  it should "tolerate whitespace between fields" in {
    validate(
      "select id,    name from collection",
      Select(Fields(Seq("id", "name")), "collection")
    )
  }

  it should "tolerate whitespace before from" in {
    validate(
      "select id, name     from collection",
      Select(Fields(Seq("id", "name")), "collection")
    )
  }

  it should "tolerate whitespace before table name" in {
    validate(
      "select id, name from          collection",
      Select(Fields(Seq("id", "name")), "collection")
    )
  }

  it should "support filtering by a hash key" in {
    validate(
      "select id, name from collection where id = 'user-id-1'",
      Select(
        Fields(Seq("id", "name")),
        "collection",
        Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
      )
    )
  }

  it should "support filtering by a hash and sort key" in {
    validate(
      "select id, name from collection where userId = 1 and id = 'user-id-1'",
      Select(
        Fields(Seq("id", "name")),
        "collection",
        Option(
          PrimaryKey(
            Key("userId", IntValue(1)),
            Some(Key("id", StringValue("user-id-1")))
          )
        )
      )
    )
  }

  it should "support double-quoted string as well" in {
    validate(
      "select id, name from collection where id = \"user-id-1\"",
      Select(
        Fields(Seq("id", "name")),
        "collection",
        Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
      )
    )
  }

  it should "tolerate spaces around limit" in {
    validate(
      "select id, name from collection    limit 10",
      Select(
        Fields(Seq("id", "name")),
        "collection",
        limit = Some(10)
      )
    )
  }

  it should "allow selecting ascending order" in {
    validate(
      "select id, name from collection asc limit 10",
      Select(
        Fields(Seq("id", "name")),
        "collection",
        None,
        Some(Ascending),
        Some(10)
      )
    )
  }

  it should "allow selecting descending order" in {
    validate(
      "select id, name from collection desc limit 10",
      Select(
        Fields(Seq("id", "name")),
        "collection",
        None,
        Some(Descending),
        Some(10)
      )
    )
  }

  it should "allow updating a field" in {
    validate(
      "update users set name = 'dave' where id = 'user-id-1'",
      Update(
        "users",
        Seq("name" -> StringValue("dave")),
        PrimaryKey(Key("id", StringValue("user-id-1")), None)
      )
    )
  }

  it should "allow updating a field with hash and sort key" in {
    validate(
      "update playlists set name = 'Chillax' where userId = 'user-id-1' and id = 1",
      Update(
        "playlists",
        Seq("name" -> StringValue("Chillax")),
        PrimaryKey(
          Key("userId", StringValue("user-id-1")),
          Some(Key("id", IntValue(1)))
        )
      )
    )
  }

  it should "allow deleting a record" in {
    validate(
      "delete from playlists where userId = 'user-id-1' and id = 1",
      Update(
        "playlists",
        Seq("name" -> StringValue("Chillax")),
        PrimaryKey(
          Key("userId", StringValue("user-id-1")),
          Some(Key("id", IntValue(1)))
        )
      )
    )
  }
}

