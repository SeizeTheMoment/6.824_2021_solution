count=50
    for i in $(seq $count); do
        go test -race
    done