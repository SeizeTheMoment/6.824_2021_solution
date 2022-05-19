count=50
    for i in $(seq $count); do
        go test -run TestManyPartitionsManyClients3A
        go test -run TestPersistPartitionUnreliable3A
    done