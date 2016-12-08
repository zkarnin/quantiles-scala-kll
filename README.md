# quantiles-scala-kll

This package implements a quantile approximation sketch analyzed in  "Optimal Quantile Approximation in Streams" by Zohar Karnin, Kevin Lang, Edo Liberty.
http://arxiv.org/abs/1603.05346

Given a stream of comparable items, given in arbitrary order, the sketch provides the quantiles of the items. The sketch is mergeable hence can be applied in a distributed environemnt.
For an error (in the rank of a given item) of e*n, with n being the total number of items, the sketch requires O(e) memory, as opposed to a random sample requiring O(e^2).

In addition to the standard quantile sketch, there is a vector version aimed for entry-wise quantiles for a stream of vector. For d-dimensional vectors the end result
is the same as having d sperate sketches, but the computation and memory overhead is significantly smaller.

Install
-------
git clone https://github.com/zkarnin/quantiles-scala-kll.git

cd quantiles-scala-kll

sbt package test

usage 
-----
Both the QuantileSketch and VectorQuantileSketch have 3 functions 
1) update: update the sketch with a single item.
2) merge: merge two sketches into a single sketch
3) quantiles(q): Produce an array containing the quantiles 1.0/q,...,1-1.0/q

Operations (1) and (2) can be used repeatedly without hurting the output of the sketch. 
Namely, they can be use in a mapreduce framework, or treeAggregate (spark) framework, etc.

