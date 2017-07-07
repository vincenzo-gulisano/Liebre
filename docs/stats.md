## Statistics

One feature that is usually useful in a Stream Processing Engine is the possibility to get some statistics about the performance of your running queries. Liebre gives you a basic set of statistics for your query. More specifically, it gives you statistics about:

1. How many tuples are added to a stream
2. How many tuples are read from a stream
3. How long it takes for a source to produce a tuple 
4. How long it takes for an operator to consume a tuple and produce the corresponding output tuples (if any)
5. How long it takes for a s sink to process each tuple

If you want Liebre to keep these statistics, you just have to do the following:

```java
q.activateStatistics(<STATS FOLDER>);
```

**Please notice:** The statistics are kept only for the streams, sources, operators and sinks you add to the query after invoking this method. This allows you to keep statistics only for some of the streams, sources, operators and sinks you define for your query.

All the statistics are produced as _csv_ files that contain 2 columns. The first column contains the timestamp (UNIX epochs in seconds) while the second column contains the number of tuples added or taken from a stream or the time (in nanos) per-uple time spent by a source, operator or sink.

### Naming convention

For each stream with id _X_, Liebre will produce two files:

1. _X_.in.csv (for the rate with which tuples are added to the stream)
2. _X_.out.csv (for the rate with which tuples are taken from the stream)

For each source, operator or sink with id _X_, Liebre will produce one file:

1. _X_.proc.csv (for the processing time of the source, operator or sink)

All the files will be produced in the folder you specify.
