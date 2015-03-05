# pagerank
We used AWS Elastic MapReduce to perform a much larger-scale computation. Actually, we computed PageRank for a reasonably 
large Web graph (685230 TODO: nodes, 7600595 TODO: edges) 
We have three different ways to calculate PageRank. 

1.simple PageRank 
A Reduce task basically just updates the PageRank value for its node based on the PageRank values of the node’s immediate 
neighbors.This direct approach took well over 20 Map Reduce passes to converge to the desired accuracy on our test graph.
It’s not very efficient. 

2.Blocked Matrix Multiplication
We can achieve much better convergence by partitioning the Web graph into Blocks, and letting each Reduce task operate on 
an entire Block at once, propagating data along multiple edges within the Block. we reduced the number of Map Reduce passes 
by doing nontrivial computation in the reduce steps. We have already used the METIS graph partitioning software to partition 
our graph into Blocks of about 10,000 nodes each. A reduce task in your Blocked PageRank computation, instead of updating the
PageRank value of a single node, will do a full PageRank computation on an entire Block of the graph, using the current 
approximate PageRank values of the block’s neighbors as fixed boundary conditions

