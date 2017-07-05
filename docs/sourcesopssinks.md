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

#### Source - TextSink

If you are writing tuples to a text file, then the TextSink allows you to minimize the information you need to provide to the query in order to instantiate such a sink. In the following example, we write tuples composed by attributes _&lt;timestamp,key,value&gt;_ to a file (We use again the MyTuple defined for the TextSink).

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

Please look at examples [1](),[2]() and [3]().