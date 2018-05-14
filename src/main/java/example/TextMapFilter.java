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
import util.Util;

public class TextMapFilter {
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

		q.activateStatistics(args[0]);

		StreamKey<MyTuple> inKey = q.addStream("in", MyTuple.class);
		StreamKey<MyTuple> mapOutKey = q.addStream("mapOut", MyTuple.class);
		StreamKey<MyTuple> outKey = q.addStream("out", MyTuple.class);

		q.addTextSource("inSource", args[1], new TextSourceFunction<MyTuple>() {
			@Override
			public MyTuple getNext(String line) {
				Util.sleep(15);
				String[] tokens = line.split(",");
				return new MyTuple(Long.valueOf(tokens[0]), Integer
						.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
			}
		}, inKey);

		q.addMapOperator("multiply", new MapFunction<MyTuple, MyTuple>() {
			@Override
			public MyTuple map(MyTuple tuple) {
				return new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2);
			}
		}, inKey, mapOutKey);

		q.addFilterOperator("filter", new FilterFunction<MyTuple>() {
			@Override
			public boolean forward(MyTuple tuple) {
				return tuple.value >= 150;
			}
		}, mapOutKey, outKey);

		q.addTextSink("outSink", args[2], new TextSinkFunction<MyTuple>() {
			@Override
			public String convertTupleToLine(MyTuple tuple) {
				return tuple.timestamp + "," + tuple.key + "," + tuple.value;
			}
		}, outKey);

		q.activate();
		Util.sleep(20000);
		q.deActivate();

	}
}