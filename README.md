# Multiway Object Pool

A concurrent object pool that supports pooling multiple resources that are associated with a single
key. This is a proof-of-concept implementation for  [@jbellis](https://github.com/jbellis) with
regards to [CASSANDRA-5661](https://issues.apache.org/jira/browse/CASSANDRA-5661).

### Usage

A pool might manage the connections for databases, such as a master and multiple slaves.

```java
MultiwayPool<String, Connection> pool = MultiwayPool.newBuilder()
    .maximumSize(50)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .build(new ResourceLifecycle<String, Connection>() {
      public Connection create(String databaseName) {
        // create connection to database
      }
      public void onRemoval(String key, Connection connection) {
        connection.close();
      }
    });

Handle<Connection> handle = pool.borrow("master");
try {
  Connection connection = handle.get();
  // use connection...
} finally {
  handle.release();
}
```
