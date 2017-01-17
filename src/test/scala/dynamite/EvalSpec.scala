package dynamite

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import dynamite.Ast.{ Query, ReplCommand }
import dynamite.Eval.{ AmbiguousIndexException, InvalidHashKeyException, UnknownIndexException, UnknownTableException }
import dynamite.Response.{ Complete, Index, KeySchema, TableDescription }
import org.scalatest._
import play.api.libs.json.Json

class EvalSpec
    extends FlatSpec
    with Matchers
    with DynamoSpec
    with TryValues
    with EitherValues {

  val tableName = "eval-spec"
  val rangeKeyTableName = "eval-spec-range-key"

  val tableNames = Seq(
    tableName,
    rangeKeyTableName
  )

  lazy val tableCache = new TableCache(Eval.describeTable(dynamo, _))

  override def beforeEach() = {
    super.beforeEach()
    tableCache.clear()
    Seed.createTable(tableName, client)
    Seed.createTableWithoutHashKey(rangeKeyTableName, client)
    Seed.insertSeedData(tableName, client)
  }

  def run(query: String): Either[Throwable, Response] =
    Parser(query).flatMap {
      case result: Query => Eval(dynamo, result, 20, tableCache).toEither
      case _: ReplCommand => ???
    }

  //TODO test non-existent table

  def validate[A](query: String, expected: A) = {
    run(query).map {
      case Response.ResultSet(pages, _) =>
        val json = pages.toList
          .map { page =>
            val result = page.result
            result.failed.foreach { error =>
              println(s"query '$query' failed: $error")
            }
            result.success.value.map(item => Json.parse(item.toString))
          }
        json should be(expected)
      case _ => ???
    }.left.map { error =>
      throw error
    }.right.value
  }

  "eval" should "select all records from dynamo" in {
    validate(s"select id, name from $tableName", List(List(
      Json.obj("id" -> 1, "name" -> "Chill Times"),
      Json.obj("id" -> 2, "name" -> "EDM4LYFE"),
      Json.obj("id" -> 3, "name" -> "Disco Fever"),
      Json.obj("id" -> 4, "name" -> "Top Pop")
    )))
  }

  it should "allow selecting all fields" in {
    validate(s"select * from $tableName", List(Seed.seedData))
  }

  it should "allow explicit ascending order" in {
    validate(
      s"select * from $tableName where userId = 'user-id-1' asc", List(
        Seed.seedData
          .filter(json => (json \ "userId").as[String] == "user-id-1")
      )
    )
  }

  it should "allow reversing the results " in {
    validate(
      s"select * from $tableName where userId = 'user-id-1' desc", List(
        Seed.seedData
        .filter(json => (json \ "userId").as[String] == "user-id-1")
        .reverse
      )
    )
  }

  // TODO: warn when fields are not present in type
  // TODO: disallow asc/desc on scan?
  // TODO: nested fields
  // TODO: catch com.amazonaws.AmazonServiceException where message contains 'Query condition missed key schema element'

  it should "select all records for a given user" in {
    validate(
      s"select name from $tableName where userId = 'user-id-1'", List(List(
        Json.obj("name" -> "Chill Times"),
        Json.obj("name" -> "EDM4LYFE")
      ))
    )
  }

  it should "limit results" in {
    validate(
      s"select name from $tableName where userId = 'user-id-1' limit 1", List(List(
        Json.obj("name" -> "Chill Times")
      ))
    )
  }

  it should "support updating a field" in {
    val newName = "Chill Timez"
    run(
      s"update $tableName set name = '$newName' where userId = 'user-id-1' and id = 1"
    ).right.value
    validate(
      s"select name from $tableName where userId = 'user-id-1' and id = 1", List(List(
        Json.obj("name" -> newName)
      ))
    )
  }

  it should "support updating without range key" in {
    val newName = "new title"
    val userId = "user-id-1"
    run(
      s"""insert into $rangeKeyTableName (userId, name) values ('$userId', "original-title")"""
    ).right.value
    run(
      s"update $rangeKeyTableName set name = '$newName' where userId = '$userId'"
    ).right.value
    validate(
      s"select name from $rangeKeyTableName where userId = '$userId'", List(List(
        Json.obj("name" -> newName)
      ))
    )
  }

  it should "support deleting a record" in {
    run(
      s"delete from $tableName where userId = 'user-id-1' and id = 1"
    ).right.value
    validate(
      s"select name from $tableName where userId = 'user-id-1' and id = 1", List(List())
    )
  }

  it should "support inserting a record" in {
    val newName = "Throwback Thursday"
    run(
      s"""insert into $tableName (userId, id, name, tracks) values ('user-id-1', 20, "$newName", [1,2,3])"""
    ).right.value
    validate(
      s"select name from $tableName where userId = 'user-id-1' and id = 20", List(List(
        Json.obj("name" -> newName)
      ))
    )
  }

  it should "support float values" in {
    val newName = "Throwback Thursday"
    run(
      s"""insert into $tableName (userId, id, name, duration) values ('user-id-1', 20, "$newName", 1.1)"""
    ).right.value
    validate(
      s"select duration from $tableName where userId = 'user-id-1' and id = 20", List(List(
        Json.obj("duration" -> 1.1)
      ))
    )
  }

  // TODO: this could use playlist-name or playlist-name-global
  it should "use an index when unambiguous" in {
    validate(s"select id from $tableName where userId = 'user-id-1' and name = 'EDM4LYFE'", List(List(
      Json.obj("id" -> 2)
    )))
  }

  it should "fail when table is unknown" in {
    run(
      s"select id from unknown-table where userId = 'user-id-1' and name = 'EDM4LYFE'"
    ).left.value shouldBe an[UnknownTableException]
  }

  it should "return ambiguous index error" in {
    run(s"select id from $tableName where userId = 'user-id-1' and duration = 10")
      .left.value shouldBe an[AmbiguousIndexException]
  }

  it should "use an explicit index when provided" in {
    validate(
      s"select id from $tableName where userId = 'user-id-1' and duration = 10 use index playlist-length-keys-only",
      List(List(
        Json.obj("id" -> 2),
        Json.obj("id" -> 1)
      ))
    )
  }

  it should "fail when index does not exist with hash provided" in {
    run(
      s"select id from $tableName where userId = 'user-id-1' use index fake-index"
    ).left.value shouldBe UnknownIndexException("fake-index")
  }

  it should "fail when index does not exist with hash and range provided" in {
    run(
      s"select id from $tableName where userId = 'user-id-1' and duration = 10 use index fake-index"
    ).left.value shouldBe UnknownIndexException("fake-index")
  }

  it should "fail when hash key does not match index hash key" in {
    run(
      s"select id from $tableName where name = 'Disco Fever' use index playlist-name"
    ).left.value shouldBe InvalidHashKeyException(
        Index(
          "playlist-name",
          KeySchema("userId", ScalarAttributeType.S), Some(KeySchema("name", ScalarAttributeType.S))
        ),
        "name"
      )
  }

  it should "query against an explicit index" in {
    validate(
      s"select id from $tableName where name = 'Disco Fever' use index playlist-name-global",
      List(List(
        Json.obj("id" -> 3)
      ))
    )
  }

  it should "query against an index when unambiguous" in {
    validate(
      s"select id from $tableName where name = 'Disco Fever'",
      List(List(
        Json.obj("id" -> 3)
      ))
    )
  }

  "aggregate" should "support count" in {
    validate(s"select count(*) from $tableName", List(List(
      Json.obj("count" -> 4)
    )))
  }

  it should "support count with filters" in {
    validate(s"select count(*) from $tableName where userId = 'user-id-1'", List(List(
      Json.obj("count" -> 2)
    )))
  }

  it should "fail on count with column name" in {
    val ex = run(s"select count(id) from $tableName where userId = 'user-id-1'").left.value
    ex.getMessage should be("Failed parsing query")
  }

  it should "support count with other fields" in {
    val ex = run(s"select count(*), name from $tableName").left.value
    ex.getMessage should be(
      "Aggregates may not be used with unaggregated fields"
    )
  }

  it should "return consumed capacity" in {
    run(
      s"select count(*) from $tableName where userId = 'user-id-1'"
    ).right.value should matchPattern {
        case Response.ResultSet(_, Some(_)) =>
      }
  }

  it should "return table description" in {
    val result = run(
      s"describe table $tableName"
    )
    result.right.value should matchPattern {
      case TableDescription(
        "eval-spec",
        KeySchema("userId", ScalarAttributeType.S),
        Some(KeySchema("id", ScalarAttributeType.N)),
        Seq(
          Index(
            "playlist-name-global",
            KeySchema("name", ScalarAttributeType.S),
            Some(KeySchema("id", ScalarAttributeType.N))
            ),
          Index(
            "playlist-length",
            KeySchema("userId", ScalarAttributeType.S),
            Some(KeySchema("duration", ScalarAttributeType.N))
            ),
          Index(
            "playlist-length-keys-only",
            KeySchema("userId", ScalarAttributeType.S),
            Some(KeySchema("duration", ScalarAttributeType.N))
            ),
          Index(
            "playlist-name",
            KeySchema("userId", ScalarAttributeType.S),
            Some(KeySchema("name", ScalarAttributeType.S))
            )
          )
        ) =>
    }
  }

  it should "render an error when table does not exist" in {
    val result = run(s"describe table non-existent-table")
    result.right.value should matchPattern {
      case Response.Info("No table exists with name non-existent-table") =>
    }
  }
}
