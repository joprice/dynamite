package dynamite

import org.scalatest._
import Ast._

class ParserSpec extends FlatSpec with Matchers {

  def validate(query: String, expected: Query) = {
    val result = Parser(query).get.value
    expected should be(result)
  }

  "parser" should "parse wildcard fields" in {
    validate("select * from playlist", Select(All, "playlist"))
  }

  "parser" should "allow dashes in ident" in {
    validate("select * from playlist-legacy", Select(All, "playlist-legacy"))
  }

  "parser" should "parse a single field" in {
    validate("select id from playlist", Select(Fields(Seq("id")), "playlist"))
  }

  it should "allow multiple fields" in {
    validate(
      "select id,name from playlist",
      Select(Fields(Seq("id", "name")), "playlist")
    )
  }

  it should "tolerate whitespace between fields" in {
    validate(
      "select id,    name from playlist",
      Select(Fields(Seq("id", "name")), "playlist")
    )
  }

  it should "tolerate whitespace before from" in {
    validate(
      "select id, name     from playlist",
      Select(Fields(Seq("id", "name")), "playlist")
    )
  }

  it should "tolerate whitespace before table name" in {
    validate(
      "select id, name from          playlist",
      Select(Fields(Seq("id", "name")), "playlist")
    )
  }

  it should "support filtering by a hash key" in {
    validate(
      "select id, name from playlist where id = 'user-id-1'",
      Select(
        Fields(Seq("id", "name")),
        "playlist",
        Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
      )
    )
  }

  it should "support filtering by a hash and sort key" in {
    validate(
      "select id, name from playlist where userId = 1 and id = 'user-id-1'",
      Select(
        Fields(Seq("id", "name")),
        "playlist",
        Option(
          PrimaryKey(
            Key("userId", IntValue("1")),
            Some(Key("id", StringValue("user-id-1")))
          )
        )
      )
    )
  }

  it should "support double-quoted string as well" in {
    validate(
      "select id, name from playlist where id = \"user-id-1\"",
      Select(
        Fields(Seq("id", "name")),
        "playlist",
        Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
      )
    )
  }

  it should "support integer values " in {
    validate(
      "select id, name from playlist where id = 1",
      Select(
        Fields(Seq("id", "name")),
        "playlist",
        Option(PrimaryKey(Key("id", IntValue("1")), None))
      )
    )
  }

  it should "support floating point values" in {
    validate(
      "update playlist set duration = 1.1 where id = 1",
      Update(
        "playlist",
        Seq("duration" -> FloatValue("1.1")),
        PrimaryKey(Key("id", IntValue("1")), None)
      )
    )
  }

  it should "support floating point values without leading integer" in {
    validate(
      "update playlist set duration = .1 where id = 1",
      Update(
        "playlist",
        Seq("duration" -> FloatValue(".1")),
        PrimaryKey(Key("id", IntValue("1")), None)
      )
    )
  }

  it should "support negative floating point values" in {
    validate(
      "update playlist set duration = -.1 where id = 1",
      Update(
        "playlist",
        Seq("duration" -> FloatValue("-.1")),
        PrimaryKey(Key("id", IntValue("1")), None)
      )
    )
  }

  it should "support negative int values" in {
    validate(
      "update playlist set duration = -1 where id = 1",
      Update(
        "playlist",
        Seq("duration" -> IntValue("-1")),
        PrimaryKey(Key("id", IntValue("1")), None)
      )
    )
  }

  it should "support array values " in {
    validate(
      "insert into playlists (id, tracks) values (1, [1,2,3])",
      Insert(
        "playlists",
        Seq(
          "id" -> IntValue("1"),
          "tracks" -> ListValue(Seq(
            IntValue("1"), IntValue("2"), IntValue("3")
          ))
        )
      )
    )
  }

  it should "support empty arrays" in {
    validate(
      "insert into playlists (id, tracks) values (1, [])",
      Insert(
        "playlists",
        Seq(
          "id" -> IntValue("1"),
          "tracks" -> ListValue(Seq.empty)
        )
      )
    )
  }

  it should "tolerate spaces around limit" in {
    validate(
      "select id, name from playlist    limit 10",
      Select(
        Fields(Seq("id", "name")),
        "playlist",
        limit = Some(10)
      )
    )
  }

  it should "allow selecting ascending order" in {
    validate(
      "select id, name from playlist asc limit 10",
      Select(
        Fields(Seq("id", "name")),
        "playlist",
        None,
        Some(Ascending),
        Some(10)
      )
    )
  }

  it should "allow selecting descending order" in {
    validate(
      "select id, name from playlist desc limit 10",
      Select(
        Fields(Seq("id", "name")),
        "playlist",
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
          Some(Key("id", IntValue("1")))
        )
      )
    )
  }

  it should "allow deleting a record" in {
    validate(
      "delete from playlists where userId = 'user-id-1' and id = 1",
      Delete(
        "playlists",
        PrimaryKey(
          Key("userId", StringValue("user-id-1")),
          Some(Key("id", IntValue("1")))
        )
      )
    )
  }

  it should "allow an item to be inserted" in {
    validate(
      "insert into playlists (userId, id) values ('user-id-1', 1)",
      Insert(
        "playlists",
        Seq(
          "userId" -> StringValue("user-id-1"),
          "id" -> IntValue("1")
        )
      )
    )
  }

  it should "allow listing of tables" in {
    validate("show tables", ShowTables)
  }

  it should "support multiline queries" in {
    validate(
      """
      |insert into playlists
      |(userId, id)
      |values ('user-id-1', 1)
      |""".stripMargin,
      Insert(
        "playlists",
        Seq(
          "userId" -> StringValue("user-id-1"),
          "id" -> IntValue("1")
        )
      )
    )
  }
}

