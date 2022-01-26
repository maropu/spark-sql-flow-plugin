## Data Lineage Example for TPC-DS queries on Neo4j Aura

You can generate a data lineage example for 103 TPC-DS queries on Neo4j Aura by using a command below:

```
$ NEO4J_AURADB_URI="neo4j+s://<your Neo4j database uri>" NEO4J_AURADB_USER="<user name>" NEO4J_AURADB_PASSWD="<password>" \
  ./build/mvn -Dtest=none -DwildcardSuites=org.apache.spark.sql.flow.TPCDSFlowWithNeo4jAuraSink test
```

Since the generated data lineage is too large to be visualized in a Neo4j browser,
you need to select a part of the data lineage that you wish to analyze, e.g.,
a CYPHER query below prints data lineage for [the TPC-DS q10 query](../src/test/resources/tpcds-flow-tests/inputs/q10.sql):

```
MATCH p=(:Table)-[:transformInto*]->(:View {name: "q10"})
WHERE ALL(r IN relationships(p) WHERE "q10" IN r.dstNodeIds)
RETURN p
```

<p align="center"><img src="tpcds-q10-neo4jaura.svg" width="800px"></p>

## List of Other Useful CYPHER Queries

### Selects the Paths Whose Reference Count Is More Than 128 in 3 Days

```
CALL {
  MATCH (n)-[t:transformInto]->(q:Query)
  WHERE duration.inDays(datetime(q.timestamp), datetime()).days <= 3
  WITH n, count(size(t.dstNodeIds)) AS refCnt, collect(q) AS qs
  WHERE refCnt > 128
  UNWIND qs AS q
  RETURN q
}
MATCH p=(s)-[*]->(q)
WHERE s:LeafPlan OR s:Table OR s:View
RETURN p
```

### Removes query nodes older than 90 days

```
// Removes older query node IDs from related relationships
MATCH (n)-[t:transformInto*]->(q:Query)
WHERE duration.inDays(datetime(q.timestamp), datetime()).days > 90
SET head(t).dstNodeIds = [uid IN head(t).dstNodeIds WHERE uid <> q.uid]
RETURN n;

// Then, removes the older nodes
MATCH (q:Query)
WHERE duration.inDays(datetime(q.timestamp), datetime()).days > 90
DETACH
DELETE q;

// Finally, removes nodes if the reference count of their relationships is 0
MATCH ()-[t:transformInto]->(n)
WHERE size(t.dstNodeIds) = 0
DETACH
DELETE n;
```
