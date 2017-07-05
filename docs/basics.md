## The basics

<p align="center">
<a><img src="https://vgulisano.files.wordpress.com/2017/07/query.jpg?w=300" alt="" width="300" height="161" /></a>
</p>

A streaming application runs a **query**, a directed acyclic graph of **sources**, **operators** and **sinks** connected by **streams**:
1. Sources produce tuples
2. Operators consume input tuple and produce output tuples
3. Sinks consume tuples

When you create your query, you add **sources**, **operators** and **sinks** and connect them with **streams**.

#### A first example

In this example, a source creates a stream of tuples with attributes _&lt;timestamp,key,value&gt;_ and feeds them to an operator that multiplies the value by 2. This operator feeds its output tuples to a sink that prints them.

*** ADD A FIGURE HERE ***

```Java
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

Query q = new Query();

StreamKey inKey = q.addStream("in", MyTuple.class);
StreamKey outKey = q.addStream("out", MyTuple.class);

q.addSource("inSource", new BaseSource() {
	Random r = new Random();

	@Override
	protected MyTuple getNextTuple() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return new MyTuple(System.currentTimeMillis(), r.nextInt(5),
			r.nextInt(100));
	}
}, inKey);

q.addOperator("countOp", new BaseOperator<MyTuple, MyTuple>() {
	@Override
	protected List processTuple(MyTuple tuple) {
		List result = new LinkedList();
		result.add(new MyTuple(tuple.timestamp, tuple.key,
			tuple.value * 2));
		return result;
	}
}, inKey, outKey);

q.addSink("outSink", new BaseSink() {
	@Override
	protected void processTuple(MyTuple tuple) {
		System.out.println(tuple.timestamp + "," + tuple.key + ","
			+ tuple.value);
	}
}, outKey);

q.activate();
try {
	Thread.sleep(5000);
} catch (InterruptedException e) {
	e.printStackTrace();
}
q.deActivate();
```

*** ADD THE PLEASE NOTICE PART ***

You can find the complete example **HERE**