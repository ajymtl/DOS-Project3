# DOS (COP-5615) Project 3

## What is working:
- Join, Routing working perfectly.

## Command to run:
`dotnet fsi --langversion:preview .\project3.fsx numNodes numRequests`

### Note: It might take a while to run. 2-3 minutes.

## Performance
Largest network I was able to deal with: 100,000 nodes.

| Number of Nodes | Number of requests | Average number of hops |
|:-:|:-:|:-:|
|256| 10 |2.77 |
|1024 | 10 |3.63 |
|16384| 10 |5.37 |
|65536| 10 |6.89 |

Bonus Implemented: No.
