# DBStress

to do:

- log database size (get from Cassandra itself?)
- log latencies and figure out some way to plot that vs. db rows
+ add brief pause between each test to reduce aborted compactions
+ consider LCS strategy for more consistent compaction behavior:

    ALTER TABLE kv WITH compaction = {'class': 'LeveledCompactionStrategy'};

+ token-aware load balancing:

    cluster := gocql.NewCluster(...)
    cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

- make sure Cassandra is configured for 8 tokens and 2 replicas
+ writes should require local quorum but reads/scans ok with one
+ move to 10K op batches, make scans do 100 parallel 10K/100 scans
