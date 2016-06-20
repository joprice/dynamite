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
          new AttributeDefinition("id", ScalarAttributeType.N),
          new AttributeDefinition("duration", ScalarAttributeType.N)
        )
        .withKeySchema(
          new KeySchemaElement("userId", KeyType.HASH),
          new KeySchemaElement("id", KeyType.RANGE)
        )
        .withLocalSecondaryIndexes(
          new LocalSecondaryIndex()
            .withIndexName("playlist-length")
            .withKeySchema(
              new KeySchemaElement("userId", KeyType.HASH),
              new KeySchemaElement("duration", KeyType.RANGE)
            )
            .withProjection(new Projection().withProjectionType(ProjectionType.ALL)),
          new LocalSecondaryIndex()
            .withIndexName("playlist-length-keys-only")
            .withKeySchema(
              new KeySchemaElement("userId", KeyType.HASH),
              new KeySchemaElement("duration", KeyType.RANGE)
            )
            .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY))
        )
    )
    TableUtils.waitUntilActive(client, tableName)
  }

  val seedData = List(
    Json.obj(
      "userId" -> "user-id-1",
      "id" -> 1,
      "name" -> "Chill Times",
      "dateCreated" -> 1,
      "duration" -> 10
    ),
    Json.obj(
      "userId" -> "user-id-1",
      "id" -> 2,
      "name" -> "EDM4LYFE",
      "dateCreated" -> 2,
      "duration" -> 10
    ),
    Json.obj(
      "userId" -> "user-id-2",
      "id" -> 3,
      "name" -> "Disco Fever",
      "dateCreated" -> 3,
      "duration" -> 5
    ),
    Json.obj(
      "userId" -> "user-id-3",
      "id" -> 4,
      "name" -> "Top Pop",
      "dateCreated" -> 4,
      "duration" -> 1
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

