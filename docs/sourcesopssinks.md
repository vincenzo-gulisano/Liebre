## Liebre sources, operators and sinks

Liebre gives you the possibility of fully defining the semantics of sources, operators and sinks. At the same time, it offers a set of common sources, operators and sinks.
Here you find their description.

#### Source - TextSource

If you are reading tuples from a text file, then the TextSource allows you to minimize the information you need to provide to the query in order to instantiate such a source. In the following example, we read tuples composed by attributes _&lt;timestamp,key,value&gt;_ from a file.

```java
class MyTuple implements Tuple {
	public long timestamp;
	public int key;
	public int value;

	public MyTuple(long timestamp, int key, int value) {
		this.timestamp = timestamp;
		this.key = key;
		this.value = value;
	}
}

q.addTextSource("inSource",
	<INPUT FILE>,
	new TextSourceFunction<MyTuple>() {
		@Override
		public MyTuple getNext(String line) {
			String[] tokens = line.split(",");
			return new MyTuple(Long.valueOf(tokens[0]), Integer
				.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
		}
	}, inKey);
```

Please notice:

1. As for the example in [the basics](), you need to specify an id for the operator you are adding (in this case _inSource_).
2. Instead of a BaseSource (as in [the basics]() example), you are now passing a _TextSourceFunction_, for which you only specify the method _getNext(String line)_.
3. As for the example in [the basics](), you need to specify the key of the stream to which the Source forwards tuples.

#### Operator - Map

The Map operator allows you to transform each input tuple into a different output tuple (possibly of a different type). In the following example we transform each input tuple of type _InputTuple_ into a tuple of type _OutputTuple_:

```java
class InputTuple implements Tuple {
	public long timestamp;
	public int key;
	public int value;

	public InputTuple(long timestamp, int key, int value) {
		this.timestamp = timestamp;
		this.key = key;
		this.value = value;
	}
}

class OutputTuple implements Tuple {
	public long timestamp;
	public int key;
	public int valueA;
	public int valueB;
	public int valueC;

	public OutputTuple(InputTuple t) {
		this.timestamp = t.timestamp;
		this.key = t.key;
		this.valueA = t.value * 2;
		this.valueB = t.value / 2;
		this.valueC = t.value + 10;
	}
}

q.addMapOperator("transform",
	new MapFunction<InputTuple, OutputTuple>() {
		@Override
		public OutputTuple map(InputTuple tuple) {
			return new OutputTuple(tuple);
		}
	}, inKey, outKey);
```

Please notice:

1. When adding a Map operator, you provide an instance of _MapFunction&lt;T1,T2&gt;_, which defines the method _public T2 map(T1 tuple)_.
2. You can of course use the same type both for the input and the output, as in the following example (we use again the MyTuple defined for the TextSink).

```java
q.addMapOperator("multiply", new MapFunction<MyTuple, MyTuple>() {
	@Override
	public MyTuple map(MyTuple tuple) {
		return new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2);
	}
}, inKey, outKey);
```

#### Operator - Filter

The Filter operator allows you to check whether a certain input tuple should be forwarded or not. In the following example, each input tuple of type _MyTuple_ is forwarded if the value is greater than:

```java
q.addFilterOperator("filter", new FilterFunction<MyTuple>() {
	@Override
	public boolean forward(MyTuple tuple) {
		return tuple.value >= 150;
	}
}, mapOutKey, outKey);
```

Please notice:

1. When adding a Filter operator, you provide an instance of _FilterFunction&lt;T&gt;_, which defines method _public boolean forward(T tuple)_.
2. Since the filter does not modify the tuple type, you only define one type (_MyTuple_) in the example

#### Sink - TextSink

If you are writing tuples to a text file, then the TextSink allows you to minimize the information you need to provide to the query in order to instantiate such a sink. In the following example, we write tuples composed by attributes _&lt;timestamp,key,value&gt;_ to a file (we use again the MyTuple defined for the TextSink).

```java
q.addTextSink(
	"outSink",
	<OUTPUT FILE>,
	new TextSinkFunction<MyTuple>() {
		@Override
		public String convertTupleToLine(MyTuple tuple) {
			return tuple.timestamp + "," + tuple.key + ","
			+ tuple.value;
		}
	}, outKey);
```

Please notice:

1. As for the example in [the basics](), you need to specify an id for the operator you are adding (in this case _inSource_).
2. Instead of a BaseSource (as in [the basics]() example), you are now passing a _TextSourceFunction_, for which you only specify the method _getNext(String line)_.
3. As for the example in [the basics](), you need to specify the key of the stream to which the Source forwards tuples.

Please look at examples [1](),[2](),[3]() and [4]().