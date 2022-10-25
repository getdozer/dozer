### Postgres Connector Sample

This sample demonstates how to utilize dozer postgres connector as an iterator.

### Example
```
let mut iterator = connector.iterator(seq_no_resolver);
  loop {
      let msg = iterator.next().unwrap();
      println("{:?}",msg);
    }
```

This sample demonstrates how postgres data can be fetched using an iterator.