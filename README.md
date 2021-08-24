# Dynamite

Dynamo query REPL

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/dd21d0c56bca4a4692139a8707bd12e7)](https://www.codacy.com/app/pricejosephd/dynamite?utm_source=github.com&utm_medium=referral&utm_content=joprice/dynamite&utm_campaign=badger)
[![CircleCI](https://circleci.com/gh/joprice/dynamite/tree/master.svg?style=svg)](https://circleci.com/gh/joprice/dynamite/tree/master)
[![codecov](https://codecov.io/gh/joprice/dynamite/branch/master/graph/badge.svg)](https://codecov.io/gh/joprice/dynamite)


## Installing

`brew tap joprice/tap && brew install dynamite`

## Querying

A limited subset of dynamo and sql are currently supported:

### Select

```sql
dql> select * from playlists limit 10;
```

When using a where clause, the table or an appropriate index will be used:

```sql
dql> select * from playlists where userId = 1 and id = 2 limit 10;
```

If there are redundant indexes or for some reason an appropriate one cannnot be found, use `use index` to provide it explicitly:

```sql
dql> select * from playlists where userId = 1 limit 10 use index profileIdIndex;
```

```sql
dql> select id, name from playlists limit 1;
```


### Insert

```sql
dql> insert into playlists (userId, id) values (1, 10);
```
### Update

```sql
dql> update playlists set name = '80s Party' where userId = 1 and id = 10;
```

### Delete

```sql
dql> delete from playlists where userId = 1 and id = 10;
```

### Show tables

```sql
dql> show tables;
```

### Describe table

```sql
dql> describe table playlists;
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

## TODO

* load dynamo client in background on startup
* order keys before other fields when `select *`
* check value type matches when building query key conditions
  - handled in select statements only
