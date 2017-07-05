package example;

import operator.map.MapFunction;
import query.Query;
import sink.text.TextSinkFunction;
import source.text.TextSourceFunction;
import stream.StreamKey;
import tuple.Tuple;

public class TextMap {
    public static void main(String[] args) {

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

	StreamKey<MyTuple> inKey = q.addStream("in", MyTuple.class);
	StreamKey<MyTuple> outKey = q.addStream("out", MyTuple.class);

	q.addTextSource("inSource",
		"/Users/vinmas/Documents/workspace_java/lepre/data/input.txt",
		new TextSourceFunction<MyTuple>() {
		    @Override
		    public MyTuple getNext(String line) {
			String[] tokens = line.split(",");
			return new MyTuple(Long.valueOf(tokens[0]), Integer
				.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
		    }
		}, inKey);

	q.addMapOperator("countOp", new MapFunction<MyTuple, MyTuple>() {
	    @Override
	    public MyTuple map(MyTuple tuple) {
		return new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2);
	    }
	}, inKey, outKey);

	q.addTextSink(
		"outSink",
		"/Users/vinmas/Documents/workspace_java/lepre/data/outputmap.txt",
		new TextSinkFunction<MyTuple>() {
		    @Override
		    public String convertTupleToLine(MyTuple tuple) {
			return tuple.timestamp + "," + tuple.key + ","
				+ tuple.value;
		    }
		}, outKey);

	q.activate();
	try {
	    Thread.sleep(5000);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
	q.deActivate();

    }
}
