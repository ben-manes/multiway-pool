# Multiway Object Pool

A concurrent object pool that supports pooling multiple resources that are associated with a single
key. This is a proof-of-concept implementation for  [@jbellis](https://github.com/jbellis) with
regards to [CASSANDRA-5661](https://issues.apache.org/jira/browse/CASSANDRA-5661).

### Usage

A pool might manage the connections for databases, such as a master and multiple slaves.

```java
LoadingMultiwayPool<String, Connection> pool = MultiwayPoolBuilder.newBuilder()
    .maximumSize(50)
    .expireAfterAccess(10, TimeUnit.MINUTES)
    .lifecycle(new ResourceLifecycle<String, Connection>() {
      public void onRemoval(String key, Connection connection) {
        connection.close();
      }
    })
    .build(new ResourceLoader<String, Connection>() {
      public Connection load(String databaseName) {
        // create connection to database
      }
    });

Connection connection = pool.borrow("master");
try {
  // use connection...
} finally {
  pool.release(connection);
}
```

Optimized using [JProfiler](http://www.ej-technologies.com/products/jprofiler/overview.html), a
full-featured Java profiler licensed freely to open source projects.
