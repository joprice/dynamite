# Queries

## Select

```tut
  import dynamite._

  Parser("select * from collection").get.value

  Parser("select * from collection where id = 1 and name = '80s hits' ").get.value
````

