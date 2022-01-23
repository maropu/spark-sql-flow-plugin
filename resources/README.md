## List of Useful CYPHER Queries

### Selects the Paths Whose Reference Count Is More Than 128 in 3 Days

```
CALL {
  MATCH (n)-[:transformInto]->(q:Query)
  WHERE duration.inDays(datetime(q.timestamp), datetime()).days <= 3
  WITH n, count(q) AS refCnt, collect(q) AS qs
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
// Decrements the reference counts of relationships related to the older query nodes
MATCH (n)-[t:transformInto*]->(q:Query)
WHERE duration.inDays(datetime(q.timestamp), datetime()).days > 90
SET head(t).refCnt = head(t).refCnt - 1
RETURN n;

// Then, removes the older nodes
MATCH (q:Query)
WHERE duration.inDays(datetime(q.timestamp), datetime()).days > 90
DETACH
DELETE q;

// Finally, removes nodes if the reference count of their relationships is 0
MATCH ()-[t:transformInto]->(n)
WHERE t.refCnt <= 0
DETACH
DELETE n;
```
