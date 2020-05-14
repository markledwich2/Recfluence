## How data is stored in blob storage ##

Example un-partitioned land & stage store
```
search
  landing
    2020-01-01_14_20_10.jsonl.gz        <- small files
    2020-01-01_14_20_13.jsonl.gz
  stage                                 <- (up too 50MB) files
    2020-01-01_14_20_13.jsonl.gz
```

Example partitions land & stage store
```
captions
  landing
    ch1
      2020-01-01_14_20_10.jsonl.gz      <- small files
      2020-01-01_14_20_13.jsonl.gz
    ch2
      2020-01-01_14_20_10.jsonl.gz
      2020-01-01_14_20_13.jsonl.gz

  stage  <- (up too 100MB) files
    ch1
      2020-01-01_14_20_16.jsonl.gz      <- grouped files within partition
    ch2
      2020-01-01_14_20_16.jsonl.gz
```

Example parititioned store
```
    ch1
      2020-01-01_14_20_10.jsonl.gz 
      2020-01-01_14_20_13.jsonl.gz
    ch2
      2020-01-01_14_20_10.jsonl.gz
      2020-01-01_14_20_13.jsonl.gz
```


