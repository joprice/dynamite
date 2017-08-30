package dynamite

import org.scalatest._
import Ast._
import EitherValues._
import dynamite.Ast.Projection.Aggregate._
import dynamite.Ast.Projection.FieldSelector._

class ParserSpec extends FlatSpec with Matchers with EitherValues {

  def parse(query: String) = {
    val result = Parser.parse(query)
    result.left.foreach { error =>
      println(error.getCause.getMessage)
    }
    result.right.value
  }

  def validate(query: String, expected: Command) = {
    parse(query) should be(expected)
  }

  "parser" should "parse wildcard fields" in {
    validate("select * from playlist", Select(Seq(All), "playlist"))
  }

  it should "allow dashes and underscores in ident" in {
    validate("select * from playlist-legacy_1", Select(Seq(All), "playlist-legacy_1"))
  }

  it should "parse a single field" in {
    validate("select id from playlist", Select(Seq(Field("id")), "playlist"))
  }

  it should "allow multiple fields" in {
    validate(
      "select id,name from playlist",
      Select(Seq(Field("id"), Field("name")), "playlist")
    )
  }

  it should "tolerate whitespace between fields" in {
    validate(
      "select id,    name from playlist",
      Select(Seq(Field("id"), Field("name")), "playlist")
    )
  }

  it should "tolerate whitespace before from" in {
    validate(
      "select id, name     from playlist",
      Select(Seq(Field("id"), Field("name")), "playlist")
    )
  }

  it should "tolerate whitespace before table name" in {
    validate(
      "select id, name from          playlist",
      Select(Seq("id", "name").map(Field), "playlist")
    )
  }

  it should "support filtering by a hash key" in {
    validate(
      "select id, name from playlist where id = 'user-id-1'",
      Select(
        Seq("id", "name").map(Field),
        "playlist",
        Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
      )
    )
  }

  it should "support filtering by a hash and sort key" in {
    validate(
      "select id, name from playlist where userId = 1 and id = 'user-id-1'",
      Select(
        Seq("id", "name").map(Field),
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
        Seq("id", "name").map(Field),
        "playlist",
        Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
      )
    )
  }

  it should "support integer values " in {
    validate(
      "select id, name from playlist where id = 1",
      Select(
        Seq("id", "name").map(Field),
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

  it should "support updating object values" in {
    validate(
      """update playlist set meta = { "tags": ["rock", "metal"], "visibility": "private" } where id = 1""",
      Update(
        "playlist",
        Seq(
          "meta" -> ObjectValue(Seq(
            "tags" -> ListValue(Seq(
              StringValue("rock"),
              StringValue("metal")
            )),
            "visibility" -> StringValue("private")
          ))
        ),
        PrimaryKey(Key("id", IntValue("1")), None)
      )
    )
  }

  it should "support tolerate whitespace in object values" in {
    validate(
      """update playlist set meta = {"tags":["rock" ,"metal"], "visibility": "private"} where id = 1""",
      Update(
        "playlist",
        Seq(
          "meta" -> ObjectValue(Seq(
            "tags" -> ListValue(Seq(
              StringValue("rock"),
              StringValue("metal")
            )),
            "visibility" -> StringValue("private")
          ))
        ),
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
        Seq("id", "name").map(Field),
        "playlist",
        limit = Some(10)
      )
    )
  }

  it should "allow selecting ascending order" in {
    validate(
      "select id, name from playlist asc limit 10",
      Select(
        Seq("id", "name").map(Field),
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
        Seq("id", "name").map(Field),
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

  it should "allow an explicit index to be used" in {
    validate(
      "select * from playlists use index playlist-length-keys-only",
      Select(Seq(All), "playlists", useIndex = Some("playlist-length-keys-only"))
    )
  }

  it should "fail on empty table name" in {
    Parser.parse("select * from ") should be('left)
  }

  it should "support count" in {
    validate("select count(*) from playlist", Select(Seq(Count), "playlist"))
  }

  it should "fail on unknown aggregate" in {
    Parser.parse("select nonexistent(*) from playlist") should be('left)
  }

  it should "support describing tables" in {
    validate("describe table playlist", DescribeTable("playlist"))
  }

  it should "ignore case" in {
    parse("SELECT * FROM playlists ASC LIMIT 1 USE INDEX playlist-length-keys-only")
    parse("SELECT COUNT(*) FROM playlists DESC LIMIT 1 USE INDEX playlist-length-keys-only")
    parse("INSERT INTO playlists (id, tracks) VALUES (1, [1,2,3])")
    parse("UPDATE playlists SET name = 'Chillax' WHERE userId = 'user-id-1' AND id = 1")
    parse("SHOW TABLES")
    parse("DESCRIBE TABLE playlist")
    parse("DELETE FROM playlists WHERE userId = 'user-id-1' AND id = 1")
    parse("FORMAT TABULAR")
    parse("FORMAT JSON")
    parse("SHOW FORMAT")
  }

  it should "support describing current format" in {
    validate("show format", ShowFormat)
  }

  it should "support setting json format" in {
    validate("format json", SetFormat(Ast.Format.Json))
  }

  it should "support setting tabular format" in {
    validate("format tabular", SetFormat(Ast.Format.Tabular))
  }

  it should "support boolean values" in {
    parse(
      """insert into playlists (userId, id, curated) values ("user-1", "id-1", true)"""
    )
    parse(
      """insert into playlists (userId, id, curated) values ("user-1", "id-1", false)"""
    )
  }
}

