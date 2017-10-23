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

import common.tuple.Tuple;
import common.util.Util;
import operator.map.MapFunction;
import query.Query;
import sink.text.TextSinkFunction;
import source.text.TextSourceFunction;
import stream.StreamKey;

public class TextMap2 {
	public static void main(String[] args) {

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

		Query q = new Query();

		q.activateStatistics(args[0]);

		StreamKey<InputTuple> inKey = q.addStream("in", InputTuple.class);
		StreamKey<OutputTuple> outKey = q.addStream("out", OutputTuple.class);

		q.addTextSource("inSource", args[1],
				new TextSourceFunction<InputTuple>() {
					@Override
					public InputTuple getNext(String line) {
						Util.sleep(50);
						String[] tokens = line.split(",");
						return new InputTuple(Long.valueOf(tokens[0]), Integer
								.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
					}
				}, inKey);

		q.addMapOperator("transform",
				new MapFunction<InputTuple, OutputTuple>() {
					@Override
					public OutputTuple map(InputTuple tuple) {
						return new OutputTuple(tuple);
					}
				}, inKey, outKey);

		q.addTextSink("outSink", args[2], new TextSinkFunction<OutputTuple>() {
			@Override
			public String convertTupleToLine(OutputTuple tuple) {
				return tuple.timestamp + "," + tuple.key + "," + tuple.valueA
						+ "," + tuple.valueB + "," + tuple.valueC;
			}
		}, outKey);

		q.activate();
		Util.sleep(40000);
		q.deActivate();

	}
}
