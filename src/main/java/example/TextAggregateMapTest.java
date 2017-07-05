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

import operator.aggregate.TimeBasedSingleWindow;
import query.Query;
import sink.text.TextSinkFunction;
import source.text.TextSourceFunction;
import stream.StreamKey;
import tuple.RichTuple;

public class TextAggregateMapTest {
	public static void main(String[] args) {

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

		Query q = new Query();

		StreamKey<InputTuple> inKey = q.addStream("in", InputTuple.class);
		StreamKey<OutputTuple> outKey = q.addStream("out", OutputTuple.class);

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

		class Win implements TimeBasedSingleWindow<InputTuple, OutputTuple> {

			private double count = 0;
			private double sum = 0;
			private long startTimestamp;
			private int key;

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
				return new OutputTuple(startTimestamp, key, (int) count,
						average);
			}

			@Override
			public TimeBasedSingleWindow<InputTuple, OutputTuple> factory(
					long timestamp, String key) {
				Win w = new Win();
				w.startTimestamp = startTimestamp;
				w.key = Integer.valueOf(key);
				return w;
			}

		}
		;
		q.addAggregateOperator("aggOp", new Win(), 7 * 24 * 3600, 24 * 3600,
				inKey, outKey);

		q.addTextSink(
				"outSink",
				"/Users/vinmas/Documents/workspace_java/lepre/data/outputaggregateandfilter.txt",
				new TextSinkFunction<OutputTuple>() {
					@Override
					public String convertTupleToLine(OutputTuple tuple) {
						return tuple.timestamp + "," + tuple.key + ","
								+ tuple.count + "," + tuple.average;
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
