# quantiles-scala-kll

This package implements a quantile approximation sketch analyzed in  "Optimal Quantile Approximation in Streams" by Zohar Karnin, Kevin Lang, Edo Liberty.
http://arxiv.org/abs/1603.05346

Given a stream of comparable items the sketch provides the quantiles of the items. The sketch is mergeable hence can be applied in a distributed environemnt.
