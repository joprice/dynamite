package dynamite

import java.util.concurrent.atomic.AtomicReference

import dynamite.Eval.AmbiguousIndexException
import org.scalatest._
import play.api.libs.json.Json

class EvalSpec
    extends FlatSpec
    with Matchers
    with DynamoSpec
    with EitherValues {

  val tableName = "eval-spec"

  val eval = Eval(client, 20, new AtomicReference(Ast.Format.Tabular))

  final case class User(name: String)

  def run(query: String): Either[Throwable, Response] =
    Parser(query).flatMap(eval.run(_).toEither)

  //TODO test non-existent table

  def validate[A](query: String, expected: A) = {
    run(query).map {
      case Response.ResultSet(result, _) =>
        val json = result.toList
          .map(_.result.get.map(item => Json.parse(item.toString)))
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
    )
    validate(
      s"select name from $tableName where userId = 'user-id-1' and id = 1", List(List(
        Json.obj("name" -> newName)
      ))
    )
  }

  it should "support deleting a record" in {
    val newName = "Chill Timez"
    run(
      s"delete from $tableName where userId = 'user-id-1' and id = 1"
    )
    validate(
      s"select name from $tableName where userId = 'user-id-1' and id = 1", List(List())
    )
  }

  it should "support inserting a record" in {
    val newName = "Throwback Thursday"
    run(
      s"""insert into $tableName (userId, id, name) values ('user-id-1', 20, "$newName")"""
    )
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
    )
    validate(
      s"select duration from $tableName where userId = 'user-id-1' and id = 20", List(List(
        Json.obj("duration" -> 1.1)
      ))
    )
  }

  it should "use an index when unambiguous" in {
    validate(s"select id from $tableName where userId = 'user-id-1' and name = 'EDM4LYFE'", List(List(
      Json.obj("id" -> 2)
    )))
  }

  it should "return ambiguous index error" in {
    run(s"select id from $tableName where userId = 'user-id-1' and duration = 10").left.value shouldBe a[AmbiguousIndexException]
  }

  it should "use an explicit index when provided" in {
    validate(s"select id from $tableName where userId = 'user-id-1' and duration = 10 use index playlist-length-keys-only", List(List(
      Json.obj("id" -> 2),
      Json.obj("id" -> 1)
    )))
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
}
