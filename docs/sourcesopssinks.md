
[![](images/liebre_small.jpg)](../index)

## Liebre sources, operators and sinks

Liebre gives you the possibility of fully defining the semantics of sources, operators and sinks. At the same time, it offers a set of common sources, operators and sinks.
Here you find their description.

#### Source - TextSource (complete example [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextMap1.java))

If you are reading tuples from a text file, then the TextSource allows you to minimize the information you need to provide to the query in order to instantiate such a source.
In the following example, we read lines from a text file and convert them to tuples composed by attributes _&lt;timestamp,key,value&gt;_ from a file.

```java
Source<String> i1 = q.addTextFileSource("I1", inputFile);

Operator<String, MyTuple> inputReader =
    q.addMapOperator(
        "map",
        line -> {
          Util.sleep(100);
          String[] tokens = line.split(",");
          return new MyTuple(
              Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
        });
```

Please notice: As for the example in [the basics](basics.md), you need to specify an id for the operator you are adding (in this case _i1_).

#### Operator - Map (complete examples [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextMap2.java) and [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextMap1.java))

The Map operator allows you to transform each input tuple into a different output tuple (possibly of a different type). In the following example, we transform each input tuple of type _InputTuple_ into a tuple of type _OutputTuple_:

```java
class MyTuple {

  public long timestamp;
  public int key;
  public int value;

  public MyTuple(long timestamp, int key, int value) {
    this.timestamp = timestamp;
    this.key = key;
    this.value = value;
  }

}

class OutputTuple {

  public long timestamp;
  public int key;
  public int valueA;
  public int valueB;
  public int valueC;

  public OutputTuple(MyTuple t) {
    this.timestamp = t.timestamp;
    this.key = t.key;
    this.valueA = t.value * 2;
    this.valueB = t.value / 2;
    this.valueC = t.value + 10;
  }
}

Operator<MyTuple, OutputTuple> transform =
    q.addMapOperator("transform", tuple -> new OutputTuple(tuple));

```

Please notice:

1. When adding a Map operator, you provide an instance of _MapFunction&lt;T1,T2&gt;_,.
2. You can of course use the same type both for the input and the output, as in the following example (we use again the MyTuple defined for the TextSource).

```java

Operator<MyTuple, MyTuple> multiply =
    q.addMapOperator(
        "multiply", tuple -> new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2));

```

#### Operator - FlatMap (complete example [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextFlatMap.java))

The FlatMap operator is similar to the Map operator, but it allows to transform each input tuple into zero, one or more output tuples. Continuing the previous example, you can add it to your query as follows.

```java
Operator<MyTuple, MyTuple> multiply = q.addFlatMapOperator("multiply", 
	tuple -> {List<MyTuple> result = new LinkedList<MyTuple>();
		result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2));
		result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 3));
		result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 4));
		return result;
    });
```

#### Operator - Filter (complete example [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextMapFilter.java))

The Filter operator allows you to check whether a certain input tuple should be forwarded or not. In the following example, each input tuple of type _MyTuple_ is forwarded if the value is greater than 150.

```java
q.addFilterOperator("filter", tuple -> tuple.value >= 150);
```

Please notice:

1. When adding a Filter operator, you provide an instance of _FilterFunction&lt;T&gt;_.
2. Since the filter does not modify the tuple type, you only define one type (_MyTuple_ in the example).

#### Operator - Router (complete example [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextRouterMap.java))

The Router operator allows you to forward each input tuple to one or multiple output streams. In the following example, each input tuple of type _MyTuple_ is forwarded to two output streams defined for the operator.

```java
Operator<MyTuple, MyTuple> router = q.addRouterOperator("router");

Operator<MyTuple, MyTuple> filterHigh = q.addFilterOperator("fHigh", t -> Integer.valueOf(t.getKey()) < 5);

Operator<MyTuple, MyTuple> filterLow = q.addFilterOperator("fLow", t -> Integer.valueOf(t.getKey()) > 4);

q.connect(router, filterHigh).connect(router, filterLow)
```

#### Operator - Aggregate (complete example [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextAggregate.java))

Differently from the Map's and the Filter's _stateless_ analysis, the Aggregate runs _stateful_ analysis.
_Stateless_ means each output tuple depends on exactly one input tuple (or to be more formal, it means that the operator does not maintain a state that evolves depending on the tuples being processed).
_Stateful_, on the other hand, means that each output tuple (potentially) depends on multiple input tuples.

The Aggregate operator aggregates multiple tuples with functions such as _sum_, _max_, _min_ or any other user-defined function. Since streams are unbounded, the aggregation is performed over _windows_.
The Aggregate operator allows for such functions to be computed over all the incoming tuples or for different _group-by_ values.

The semantics of streaming aggregation depend on the type and behavior of its _window_, while the memory footprint and the processing cost depend on its internal implementation. Many variations have been discussed in the literature. 
The Aggregate currently provided by Liebre is for time-based sliding windows and is implemented maintaining a single window for each distinct group-by value.
In the following, we build an example step-by-step.

For an Aggregate operator, our input and output tuples must implement the **RichTuple** interface:

```java
class InputTuple implements RichTuple {
	public long timestamp;
	public int key;
	public int value;

	public InputTuple(long timestamp, int key, int value) {
		this.timestamp = timestamp;
		this.key = key;
		this.value = value;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String getKey() {
		return key + "";
	}
}

class OutputTuple implements RichTuple {
	public long timestamp;
	public int key;
	public int count;
	public double average;

	public OutputTuple(long timestamp, int key, int count,
		double average) {
		this.timestamp = timestamp;
		this.key = key;
		this.count = count;
		this.average = average;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String getKey() {
		return key + "";
	}
}
```

Please notice:

1. You need to define a _long getTimestamp()_ method, because of the time-based sliding windows.
2. You need to define a _String getKey()_ so that tuples giving the same key are aggregated together. If you want to aggregate all tuples together (not by key) you can just return an empty String.

For your convenience, you can also extend the BaseRichTuple class, which defines fields and method to keep the _timestamp_ and _key_ fields:

```java
class InputTuple extends BaseRichTuple {
	public int value;

	public InputTuple(long timestamp, int key, int value) {
		super(timestamp, key + "");
		this.value = value;
	}
}

class OutputTuple extends BaseRichTuple {
	public int count;
	public double average;

	public OutputTuple(long timestamp, int key, int count,
		double average) {
		super(timestamp, key + "");
		this.count = count;
		this.average = average;
	}
}
```

Once the input and output tuples types are defined, you can specify the function you will use to aggregate the data. In the following example, our window will count the tuples observed in the window and also compute the average for the field _value_:

```java
class AverageWindow extends BaseTimeBasedSingleWindow<InputTuple, OutputTuple> {

	private double count = 0;
	private double sum = 0;

	@Override
	public void add(InputTuple t) {
		count++;
		sum += t.value;
	}

	@Override
	public void remove(InputTuple t) {
		count--;
		sum -= t.value;
	}

	@Override
	public OutputTuple getAggregatedResult() {
		double average = count > 0 ? sum / count : 0;
		return new OutputTuple(startTimestamp, Integer.valueOf(key),
			(int) count, average);
	}

	@Override
	public TimeBasedSingleWindow<InputTuple, OutputTuple> factory() {
		return new Win();
	}

}
```

Please notice:

1. You provide the implementation for the methods invoked to add and remove tuples from the window (_void add(InputTuple t)_ and _void remove(InputTuple t)_).
2. You specify how to produce the output tuple (notice fields _startTimestamp_ - the start timestamp of the window - and _key_ are given to you by the class _BaseTimeBasedSingleWindow_).
3. You also define the method _factory()_, which you can use to initialize your variables (if needed).

Once you define the types for the input and output tuples and the window, you can then add the aggregate to the query:


```java
q.addAggregateOperator("aggOp", new AverageWindow(), WINDOW_SIZE, WINDOW_SLIDE)
```

As shown, aside from the id of the operator and an instance of the window, you also specify the window size and the window advance, using the same unit of measure used by your tuples. For instance, if the timestamp of the tuples are in seconds, the following aggregate defined a window of size 4 weeks and advance 1 week:

```java
q.addAggregateOperator("aggOp", new AverageWindow(), 4 * 7 * 24 * 3600,
	7 * 24 * 3600);	
```

##### **Please notice:** The Aggregate enforces deterministic processing. Because of this, it assumes **tuples are fed in timestamp order**.

#### Operator - Join (complete example [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextJoin.java))

Similarly to the Aggregate, the Join is a _stateful_ operator. It compares tuples from 2 streams with a given predicate and forwards an output tuple every time the predicate holds. For this operator, you can specify different types for the two input tuples and for the output one. Since the join also operates on _time-based sliding windows_, these types should implement _RichTuple_ or extend _BaseRichTuple_:

```java
class InputTuple1 extends BaseRichTuple {

	public int a;

	public InputTuple1(long timestamp, int a) {
		super(timestamp, "");
		this.a = a;
	}

}

class InputTuple2 extends BaseRichTuple {

	public int b;

	public InputTuple2(long timestamp, int b) {
		super(timestamp, "");
		this.b = b;
	}

}

class OutputTuple extends BaseRichTuple {

	public InputTuple1 t1;
	public InputTuple2 t2;

	public OutputTuple(long timestamp, InputTuple1 t1, InputTuple2 t2) {
		super(timestamp, "");
		this.t1 = t1;
		this.t2 = t2;
	}

}
```

Please notice: Since the field _key_ is not used in this example, we simply set an empty string for it.

Once the types for input and output tuples are defined, we can proceed adding the join operator to the query. In the example, we want to produce an output tuple carrying each pair of input tuples for which _a &lt; b_:

```java
q.addJoinOperator("join",
    new JoinFunction<InputTuple1, InputTuple2, OutputTuple>() {
      @Override
      public OutputTuple apply(InputTuple1 t1, InputTuple2 t2) {
        if (t1.a < t2.b) {
          return new OutputTuple(t1.getTimestamp(), t1, t2);
        }
        return null;
      }
    }, 10000);
```

Please notice:

1. Since the join operates on sliding windows, we specify the window size (10 seconds in the example, because the unit of time for the tuples is millisecond).
2. Also the Join enforces deterministic processing. Because of this, it assumes **tuples are fed in timestamp order** from each input stream.

#### Sink - TextSink (complete example [here](https://github.com/vincenzo-gulisano/Liebre/blob/master/src/test/java/example/TextMap1.java))

If you are writing tuples to a text file, then the TextSink allows you to minimize the information you need to provide to the query in order to instantiate such a sink. In the following example, we write tuples composed by attributes _&lt;timestamp,key,value&gt;_ to a file (we use again the MyTuple defined for the TextSink).

```java
q.addTextFileSink("o1", outputFile, true);
```

Please notice: as for the example in [the basics](), you need to specify an id for the operator you are adding (in this case _o1_).
