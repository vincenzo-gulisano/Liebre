/*  Copyright (C) 2017  Vincenzo Gulisano
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package example;

import operator.filter.FilterFunction;
import operator.map.MapFunction;
import query.Query;
import sink.text.TextSinkFunction;
import source.text.TextSourceFunction;
import stream.StreamKey;
import tuple.Tuple;

public class TextMapFilterTest {
    public static void main(String[] args) {

	/*
	 * Input and output data types
	 */

	// TODO timestamp, key, value and change accordingly!!!
	
	class InputTuple implements Tuple {
	    public long timestamp;
	    public int a;
	    public int b;

	    public InputTuple(long timestamp, int a, int b) {
		this.timestamp = timestamp;
		this.a = a;
		this.b = b;
	    }
	}

	class OutputTuple implements Tuple {
	    public long timestamp;
	    public int sum;

	    public OutputTuple(long timestamp, int sum) {
		this.timestamp = timestamp;
		this.sum = sum;
	    }
	}

	Query q = new Query();

	StreamKey<InputTuple> inKey = q.addStream("in", InputTuple.class);
	StreamKey<OutputTuple> mapOutKey = q.addStream("mapOut",
		OutputTuple.class);
	StreamKey<OutputTuple> outKey = q.addStream("out", OutputTuple.class);

	// Source
	q.addTextSource("inSource",
		"/Users/vinmas/Documents/workspace_java/lepre/data/input.txt",
		new TextSourceFunction<InputTuple>() {
		    @Override
		    public InputTuple getNext(String line) {
			String[] tokens = line.split(",");
			return new InputTuple(Long.valueOf(tokens[0]), Integer
				.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
		    }
		}, inKey);

	// Map
	q.addMapOperator("countOp", new MapFunction<InputTuple, OutputTuple>() {
	    @Override
	    public OutputTuple map(InputTuple tuple) {
		return new OutputTuple(tuple.timestamp, tuple.a + tuple.b);
	    }
	}, inKey, mapOutKey);

	// Filter
	q.addFilterOperator("filter", new FilterFunction<OutputTuple>() {
	    @Override
	    public boolean forward(OutputTuple tuple) {
		return tuple.sum >= 150;
	    }
	}, mapOutKey, outKey);

	/*
	 * Sink
	 */

	q.addTextSink(
		"outSink",
		"/Users/vinmas/Documents/workspace_java/lepre/data/outputmapandfilter.txt",
		new TextSinkFunction<OutputTuple>() {
		    @Override
		    public String convertTupleToLine(OutputTuple tuple) {
			return tuple.timestamp + "," + tuple.sum;
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
