# Dynamite

Dynamo query REPL

[![CircleCI](https://circleci.com/gh/joprice/dynamite/tree/master.svg?style=svg)](https://circleci.com/gh/joprice/dynamite/tree/master)

## Installing

`brew tap joprice/tap && brew install dynamite`

## Querying

A limited subset of dynamo and sql are currently supported:

### Select

```sql
dql> select * from playlists limit 10
```

```sql
dql> select * from playlists where userId = 1 and id = 2 limit 10
```

```sql
dql> select id, name from playlists limit 1
```

### Insert

```sql
dql> insert into playlists (userId, id) values (1, 10)
```
### Update

```sql
dql> update playlists set name = '80s Party' where userId = 1 and id = 10
```

### Delete

```sql
dql> delete from playlists where userId = 1 and id = 10
```

### Show tables

```sql
dql> show tables
```

## Scripting

Dynamite can also be used to run a single script:

```bash
dynamite < query.dql 
```

By default, the output is the same as the repl output. This can also be set
to 'json' or 'json-pretty':

```bash
dynamite --format=json < query.dql 

dynamite --format=json-pretty < query.dql 
```


