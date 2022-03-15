# Benchmark For Level Down Complex Queries

This repository contains a benchmark of several strategies of querying for a complex data with Level Down.

## The Data

We try to store HTTP history data which includes:

- The URL
- The HTTP method
- Optional HTTP headers
- Optional payload message
- The application ID that generated the data (the `app`).
- The user ID that generated the data (the `user`)

### Tuple 1

This tuple contains data used by Method #1, #2, and #3.

```json
{
  "key": "the same as insert key",
  "app": "The ID of the application, random string of 6",
  "user": "The ID of the user, UUID",
  "date": "ISO date string, not related to the queries and processing, used for quality control",
  "request": { "": "The HTTP request data" }
}
```

### Indexed query tuples

The Method 4 uses 3 tuples to store the data. Tuple #2 stores the HTTP request data. Tuple #3 has keys allowing to iterate over users in a chronological order. Tuple #4 has keys allowing to iterate over apps in a chronological order.
Tuples #3 and #4 values are keys to the Tuple #1.

```plain
Key #2 (data): [ISO time] + "~" + [USER ID]
```

```plain
Key #3 (users): "~" + [USER ID] + "~" + [ISO time] + "~"
```

```plain
Key #4 (apps): "~" +  [APP ID] + "~" + [ISO time] + "~"
```

Note, Key #2 may contain anything other than [USER ID]. It is to avoid collisions when two users send the data at the same time.

## Query Methods

### Method #1: Iterate Keys

This method reads keys and values from the store while iterating over the tuple. If querying for an app / user the key is split into parts (separated by the tilde "~"). If the query value match target field then the value is processed (deserialized to an object) and added to the list of results.

- always reads keys
- always reads values
- only parses values when needed to returns

### Method #2: Iterate Values

This method ignores keys entirely. It reads the data and parses it. If querying for an app / use it compares the corresponding value in the read object.

- never reads keys
- always reads values
- always parses values

### Method #3: Iterate Keys, Values on-demand

Two-part query. First, it reads keys only and iterates over them as Method #1. If the key passes the filter function it is asses to the list of keys. After limit is reached values are read from the tuple in a bulk operation.

- always reads keys
- reads values only when needed
- parses values for final result only.

### Method #4: Index tables

### Scenario #1: Listing all data

It iterates over all data from the Tuple 2 (the data) until the limit is reached or iterator finish. All data are parsed and returned.

### Scenario #2: Listing user data

It iterates over the Tuple #3 querying for keys `gte` equal `~[USER ID]~` and `lte` equal `~[USER ID]~~`. These properties are set on the level down iterator object.
In this scenario both keys and values are read from the table. Keys are read to return the last read key for pagination and the value if the key of the data in the Tuple #2.

After the index list is read then it reads data in a bulk operation returning the data.

## The winner

On small sets of data (~100) the difference between methods are minimal. However, Method #1 and #4 are leaning towards fastest methods.

On a medium data sample (few thousand) and large data sample (hundreds of thousands) the worst performance manifests Method #2 and the best Method #4.

See the medians for paginating over the entire database:

Method #1: 20.43 ms

Method #2: 23.96 ms

Method #3: 11.42 ms

Method #4: 5.42 ms

This means that it takes about 5.42 ms Method #4 to query for another page of results.

Also note, medians are similar even when drastically increasing the amount of data (from thousands to hundred of thousands).

## Conclusion

Method #4 that creates multiple tables and stores that data in one of them and then references in other tuples, is by far the most efficient method for querying for a complex data in the level database. However, it takes must more effort to prepare the indexes and to manage them when adding / deleting the data.

If a difference in making a single query of ~6 ms makes not a big deal to your system, it might be more suitable to use Method #3. It operates on the same data as Method #1 and #2 but it optimises the way how the value is read which takes the biggest tool on the performance.

## Running The Benchmark

Step 1: Prepare the data

```sh
node ./index.js --generate
```

Note, by default it creates 100 000 records. You can adjust this value in the `addHistory()` method.

Step 2: Run the query

```sh
node ./index.js --all --app --user
```

The `--all` queries for all history data. `--app` allows to query for a specific application data. `--user` queries for a specific user data.
