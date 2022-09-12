### Dozer Experience 
```
Dozer
.initialize_sources([])
.initialize_query("""
select * from actor
""")
.initialize_transformation({})
.serve();
```

```
Dozer::Pipeline.initialize({
  nodes: [],
  edges: []
})
```