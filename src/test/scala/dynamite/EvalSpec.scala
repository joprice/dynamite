package dynamite

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import org.scalatest._
import play.api.libs.json.{ JsValue, Json }
import scala.collection.JavaConverters._
import scala.concurrent.duration._

trait DynamoTestClient {
  val dynamoPortKey = "dynamodb.local.port"

  val dynamoPort = sys.props.get(dynamoPortKey).getOrElse {
    throw new Exception(s"Failed to find $dynamoPortKey")
  }

  lazy val config = new ClientConfiguration()
    .withConnectionTimeout(1.second.toMillis.toInt)
    .withSocketTimeout(1.second.toMillis.toInt)
  lazy val client = new AmazonDynamoDBClient(new BasicAWSCredentials("", ""), config)
    .withEndpoint[AmazonDynamoDBClient](s"http://localhost:$dynamoPort")
}

class EvalSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with DynamoTestClient {

  case class User(name: String)

  Seed.createTable(client)

  val eval = Eval(client)
  val dynamo = new DynamoDB(client)
  val table = dynamo.getTable(Seed.tableName)

  override def beforeEach() = Seed.insertSeedData(client)

  override def afterAll() = {
    table.delete()
    table.waitForDelete()
    client.shutdown()
  }

  def run(query: String) = eval.run(Parser(query).right.get).get

  def runQuery(query: String) = {
    val Response.ResultSet(result, _) = run(query)
    result
  }

  def runUpdate(query: String) = run(query)

  //TODO test non-existent table

  def validate(query: String, expected: List[List[JsValue]]) = {
    val result = runQuery(query)
    val json = result.toList
      .map(_.result.get.map(item => Json.parse(item.toJSON)))
    json should be(expected)
  }

  "eval" should "select all records from dynamo" in {
    validate("select id, name from playlists", List(List(
      Json.obj("id" -> 1, "name" -> "Chill Times"),
      Json.obj("id" -> 2, "name" -> "EDM4LYFE"),
      Json.obj("id" -> 3, "name" -> "Disco Fever"),
      Json.obj("id" -> 4, "name" -> "Top Pop")
    )))
  }

  it should "allow selecting all fields" in {
    validate("select * from playlists", List(Seed.seedData))
  }

  it should "allow reversing the results " in {
    validate(
      "select * from playlists where userId = 'user-id-1' desc", List(
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
      "select name from playlists where userId = 'user-id-1'", List(List(
        Json.obj("name" -> "Chill Times"),
        Json.obj("name" -> "EDM4LYFE")
      ))
    )
  }

  it should "limit results" in {
    validate(
      "select name from playlists where userId = 'user-id-1' limit 1", List(List(
        Json.obj("name" -> "Chill Times")
      ))
    )
  }

  it should "support updating a field" in {
    val newName = "Chill Timez"
    runUpdate(
      s"update playlists set name = '$newName' where userId = 'user-id-1' and id = 1"
    )
    validate(
      "select name from playlists where userId = 'user-id-1' and id = 1", List(List(
        Json.obj("name" -> newName)
      ))
    )
  }

  it should "support deleting a record" in {
    val newName = "Chill Timez"
    runUpdate(
      s"delete from playlists where userId = 'user-id-1' and id = 1"
    )
    validate(
      "select name from playlists where userId = 'user-id-1' and id = 1", List(List())
    )
  }

  it should "support inserting a record" in {
    val newName = "Throwback Thursday"
    runUpdate(
      s"""insert into playlists (userId, id, name) values ('user-id-1', 20, "$newName")"""
    )
    validate(
      "select name from playlists where userId = 'user-id-1' and id = 20", List(List(
        Json.obj("name" -> newName)
      ))
    )
  }

  it should "support float values" in {
    val newName = "Throwback Thursday"
    runUpdate(
      s"""insert into playlists (userId, id, name, duration) values ('user-id-1', 20, "$newName", 1.1)"""
    )
    validate(
      "select duration from playlists where userId = 'user-id-1' and id = 20", List(List(
        Json.obj("duration" -> 1.1)
      ))
    )
  }

  it should "use an index when available" in {
    def result = runQuery("select id from playlists where userId = 'user-id-1' and duration = 10")
    an[AmbiguousIndexException] should be thrownBy result
  }

  it should "use an explicit index when provided" in {
    validate("select id from playlists where userId = 'user-id-1' and duration = 10 use index playlist-length-keys-only", List(List(
      Json.obj("id" -> 2),
      Json.obj("id" -> 1)
    )))
  }

  //  it should "use an index when unambiguous" in {
  //    validate("select id from playlists where userId = 'user-id-1' and duration = 10", List(List(
  //      Json.obj("id" -> 2),
  //      Json.obj("id" -> 1)
  //    )))
  //  }
}
