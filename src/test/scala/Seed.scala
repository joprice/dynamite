package dynamite

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.{ DynamoDB, Item }
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils
import play.api.libs.json.Json

object Seed {

  val tableName = "playlists"

  def createTable(client: AmazonDynamoDB): Unit = {
    TableUtils.createTableIfNotExists(
      client,
      new CreateTableRequest()
        .withTableName(tableName)
        .withProvisionedThroughput(
          new ProvisionedThroughput()
            .withReadCapacityUnits(5L)
            .withWriteCapacityUnits(5L)
        )
        .withAttributeDefinitions(
          new AttributeDefinition("userId", ScalarAttributeType.S),
          new AttributeDefinition("id", ScalarAttributeType.N)
        )
        .withKeySchema(
          new KeySchemaElement("userId", KeyType.HASH),
          new KeySchemaElement("id", KeyType.RANGE)
        )
    )
    TableUtils.waitUntilActive(client, tableName)
  }

  val seedData = List(
    Json.obj(
      "userId" -> "user-id-1",
      "id" -> 1,
      "name" -> "Chill Times",
      "dateCreated" -> 1
    ),
    Json.obj(
      "userId" -> "user-id-1",
      "id" -> 2,
      "name" -> "EDM4LYFE",
      "dateCreated" -> 2
    ),
    Json.obj(
      "userId" -> "user-id-2",
      "id" -> 1,
      "name" -> "Disco Fever",
      "dateCreated" -> 3
    ),
    Json.obj(
      "userId" -> "user-id-3",
      "id" -> 1,
      "name" -> "Top Pop",
      "dateCreated" -> 4
    )
  )

  def insertSeedData(client: AmazonDynamoDB) = {
    val dynamo = new DynamoDB(client)
    val table = dynamo.getTable(tableName)
    val items = seedData.map(json => Item.fromJSON(Json.stringify(json)))
    items.foreach(table.putItem(_))
  }

  def apply(client: AmazonDynamoDB): Unit = {
    //TableUtils.deleteTableIfExists(client, new DeleteTableRequest().withTableName("users"))
    createTable(client)
    insertSeedData(client)
  }
}

