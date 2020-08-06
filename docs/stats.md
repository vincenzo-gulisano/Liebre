
[![](images/liebre_small.jpg)](../index)

## Statistics

One useful feature of a stream processing engine is the possibility to get statistics about the performance of your running queries. Liebre gives you a basic set of statistics for your query. More specifically, it gives you statistics about:

1. How many tuples are added to a stream.
2. How many tuples are read from a stream.
3. How many tuples are processed by an operator
3. How long it takes for a source to produce a tuple. 
4. How long it takes for an operator to consume a tuple and produce the corresponding output tuples (if any).
5. How long it takes for a sink to process each tuple.

If you want Liebre to keep these statistics, then add this to your program:

```java
LiebreContext.setOperatorMetrics(Metrics.file(reportFolder));
LiebreContext.setStreamMetrics(Metrics.file(reportFolder));
```

**Please notice:** The statistics are kept only for the streams, sources, operators and sinks you add to the query after invoking these methods. This allows you to keep statistics only for some of the streams, sources, operators and sinks of your query.

All the statistics are produced as _csv_ files that contain 2 columns. The first column contains the timestamp (UNIX epochs in seconds) while the second column contains the number of tuples added or taken from a stream or the per-tuple time (in nanoseconds) spent by a source, operator or sink.

### Naming convention

For each stream with id _X_, Liebre will produce two files:

1. _X_.IN.csv (for the rate with which tuples are added to the stream).
2. _X_.OUT.csv (for the rate with which tuples are taken from the stream).

For each source, operator or sink with id _X_, Liebre will produce one file:

1. _X_.EXEC.csv (for the processing time of the source, operator or sink)
2. _X_.RATE.csv (for the rate of the source, operator or sink)

All the files will be produced in the folder you specify.

### User defined metrics

Liebre also allows you to define your own metrics. If you want to maintain a count, for instance, that is logged every second, you can do it like this:

```java
throughputMetric = LiebreContext.userMetrics().newCountPerSecondMetric(id, "rate"););
```

In this case, you can register the user-defined metrics like this:

```java
LiebreContext.setUserMetrics(Metrics.file(reportFolder));
```
