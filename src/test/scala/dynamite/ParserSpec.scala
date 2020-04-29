package dynamite

import zio.test._
import zio.test.Assertion._
import Ast._
import dynamite.Ast.Projection.Aggregate._
import dynamite.Ast.Projection.FieldSelector._

object ParserSpec extends DefaultRunnableSpec {

  def parse(query: String) = Parser.parse(query)

  def parseSuccess(query: String) =
    assert(parse(query))(isRight(anything))

  def validate(query: String, expected: Command) =
    assert(parse(query).left.map(_.getCause.getMessage))(
      isRight(equalTo(expected))
    )

  def spec = suite("parser")(
    test("parse wildcard fields")(
      validate("select * from playlist", Select(Seq(All), "playlist"))
    ),
    test("allow dashes and underscores in ident")(
      validate(
        "select * from playlist-legacy_1",
        Select(Seq(All), "playlist-legacy_1")
      )
    ),
    test("parse a single field")(
      validate("select id from playlist", Select(Seq(Field("id")), "playlist"))
    ),
    test("allow multiple fields")(
      validate(
        "select id,name from playlist",
        Select(Seq(Field("id"), Field("name")), "playlist")
      )
    ),
    test("tolerate whitespace between fields")(
      validate(
        "select id,    name from playlist",
        Select(Seq(Field("id"), Field("name")), "playlist")
      )
    ),
    test("tolerate whitespace before from")(
      validate(
        "select id, name     from playlist",
        Select(Seq(Field("id"), Field("name")), "playlist")
      )
    ),
    test("tolerate whitespace before table name")(
      validate(
        "select id, name from          playlist",
        Select(Seq("id", "name").map(Field), "playlist")
      )
    ),
    test("support filtering by a hash key")(
      validate(
        "select id, name from playlist where id = 'user-id-1'",
        Select(
          Seq("id", "name").map(Field),
          "playlist",
          Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
        )
      )
    ),
    test("support filtering by a hash and sort key")(
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
    ),
    test("support double-quoted string as well")(
      validate(
        "select id, name from playlist where id = \"user-id-1\"",
        Select(
          Seq("id", "name").map(Field),
          "playlist",
          Option(PrimaryKey(Key("id", StringValue("user-id-1")), None))
        )
      )
    ),
    test("support integer values ")(
      validate(
        "select id, name from playlist where id = 1",
        Select(
          Seq("id", "name").map(Field),
          "playlist",
          Option(PrimaryKey(Key("id", IntValue("1")), None))
        )
      )
    ),
    test("support floating point values")(
      validate(
        "update playlist set duration = 1.1 where id = 1",
        Update(
          "playlist",
          Seq("duration" -> FloatValue("1.1")),
          PrimaryKey(Key("id", IntValue("1")), None)
        )
      )
    ),
    test("support floating point values without leading integer")(
      validate(
        "update playlist set duration = .1 where id = 1",
        Update(
          "playlist",
          Seq("duration" -> FloatValue(".1")),
          PrimaryKey(Key("id", IntValue("1")), None)
        )
      )
    ),
    test("support negative floating point values")(
      validate(
        "update playlist set duration = -.1 where id = 1",
        Update(
          "playlist",
          Seq("duration" -> FloatValue("-.1")),
          PrimaryKey(Key("id", IntValue("1")), None)
        )
      )
    ),
    test("support negative int values")(
      validate(
        "update playlist set duration = -1 where id = 1",
        Update(
          "playlist",
          Seq("duration" -> IntValue("-1")),
          PrimaryKey(Key("id", IntValue("1")), None)
        )
      )
    ),
    test("support updating object values")(
      validate(
        """update playlist set meta = { "tags": ["rock", "metal"], "visibility": "private" } where id = 1""",
        Update(
          "playlist",
          Seq(
            "meta" -> ObjectValue(
              Seq(
                "tags" -> ListValue(
                  Seq(
                    StringValue("rock"),
                    StringValue("metal")
                  )
                ),
                "visibility" -> StringValue("private")
              )
            )
          ),
          PrimaryKey(Key("id", IntValue("1")), None)
        )
      )
    ),
    test("support tolerate whitespace in object values")(
      validate(
        """update playlist set meta = {"tags":["rock" ,"metal"], "visibility": "private"} where id = 1""",
        Update(
          "playlist",
          Seq(
            "meta" -> ObjectValue(
              Seq(
                "tags" -> ListValue(
                  Seq(
                    StringValue("rock"),
                    StringValue("metal")
                  )
                ),
                "visibility" -> StringValue("private")
              )
            )
          ),
          PrimaryKey(Key("id", IntValue("1")), None)
        )
      )
    ),
    test("support array values ")(
      validate(
        "insert into playlists (id, tracks) values (1, [1,2,3])",
        Insert(
          "playlists",
          Seq(
            "id" -> IntValue("1"),
            "tracks" -> ListValue(
              Seq(
                IntValue("1"),
                IntValue("2"),
                IntValue("3")
              )
            )
          )
        )
      )
    ),
    test("support empty arrays")(
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
    ),
    test("tolerate spaces around limit")(
      validate(
        "select id, name from playlist    limit 10",
        Select(
          Seq("id", "name").map(Field),
          "playlist",
          limit = Some(10)
        )
      )
    ),
    test("allow selecting ascending order")(
      validate(
        "select id, name from playlist order by id asc limit 10",
        Select(
          Seq("id", "name").map(Field),
          "playlist",
          None,
          Some(OrderBy("id", Some(Ascending))),
          Some(10)
        )
      )
    ),
    test("allow selecting descending order")(
      validate(
        "select id, name from playlist order by name desc limit 10",
        Select(
          Seq("id", "name").map(Field),
          "playlist",
          None,
          Some(OrderBy("name", Some(Descending))),
          Some(10)
        )
      )
    ),
    test("allow updating a field")(
      validate(
        "update users set name = 'dave' where id = 'user-id-1'",
        Update(
          "users",
          Seq("name" -> StringValue("dave")),
          PrimaryKey(Key("id", StringValue("user-id-1")), None)
        )
      )
    ),
    test("allow updating a field with hash and sort key")(
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
    ),
    test("allow deleting a record")(
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
    ),
    test("allow an item to be inserted")(
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
    ),
    test("allow listing of tables")(
      validate("show tables", ShowTables)
    ),
    test("support multiline queries")(
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
    ),
    test("allow an explicit index to be used")(
      validate(
        "select * from playlists use index playlist-length-keys-only",
        Select(
          Seq(All),
          "playlists",
          useIndex = Some("playlist-length-keys-only")
        )
      )
    ),
    test("fail on empty table name")(
      assert(Parser.parse("select * from ").left.map(_.getMessage))(
        isLeft(equalTo("Failed parsing query"))
      )
    ),
    test("support count")(
      validate("select count(*) from playlist", Select(Seq(Count), "playlist"))
    ),
    test("fail on unknown aggregate")(
      assert(
        Parser
          .parse("select nonexistent(*) from playlist")
          .left
          .map(_.getMessage)
      )(isLeft(equalTo("Failed parsing query")))
    ),
    test("support describing tables")(
      validate("describe table playlist", DescribeTable("playlist"))
    ),
    test("ignore case")(
      parseSuccess(
        "SELECT * FROM playlists ORDER BY ID ASC LIMIT 1 USE INDEX playlist-length-keys-only"
      ) &&
        parseSuccess(
          "SELECT COUNT(*) FROM playlists ORDER BY ID DESC LIMIT 1 USE INDEX playlist-length-keys-only"
        ) &&
        parseSuccess("INSERT INTO playlists (id, tracks) VALUES (1, [1,2,3])") &&
        parseSuccess(
          "UPDATE playlists SET name = 'Chillax' WHERE userId = 'user-id-1' AND id = 1"
        ) &&
        parseSuccess("SHOW TABLES") &&
        parseSuccess("DESCRIBE TABLE playlist") &&
        parseSuccess(
          "DELETE FROM playlists WHERE userId = 'user-id-1' AND id = 1"
        ) &&
        parseSuccess("FORMAT TABULAR") &&
        parseSuccess("FORMAT JSON") &&
        parseSuccess("SHOW FORMAT")
    ),
    test("support describing current format")(
      validate("show format", ShowFormat)
    ),
    test("support setting json format")(
      validate("format json", SetFormat(Ast.Format.Json))
    ),
    test("support setting tabular format")(
      validate("format tabular", SetFormat(Ast.Format.Tabular))
    ),
    test("support true")(
      validate(
        """insert into playlists (userId, id, curated) values ("user-1", "id-1", true)""",
        Insert(
          "playlists",
          Seq(
            "userId" -> StringValue("user-1"),
            "id" -> StringValue("id-1"),
            "curated" -> BoolValue(true)
          )
        )
      )
    ),
    test("support false")(
      validate(
        """insert into playlists (userId, id, curated) values ("user-1", "id-1", false)""",
        Insert(
          "playlists",
          Seq(
            "userId" -> StringValue("user-1"),
            "id" -> StringValue("id-1"),
            "curated" -> BoolValue(false)
          )
        )
      )
    ),
    test("support false")(
      validate(
        """create table users(userId string)""",
        CreateTable(
          tableName = "users",
          name = "userId",
          typeName = "string"
        )
      )
    )
  )
}
