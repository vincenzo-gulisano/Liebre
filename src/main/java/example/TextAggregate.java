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

import operator.aggregate.BaseTimeBasedSingleWindow;
import operator.aggregate.TimeBasedSingleWindow;
import query.Query;
import sink.text.TextSinkFunction;
import source.text.TextSourceFunction;
import stream.StreamKey;
import tuple.RichTuple;
import util.Util;

public class TextAggregate {
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

		q.activateStatistics(args[0]);

		StreamKey<InputTuple> inKey = q.addStream("in", InputTuple.class);
		StreamKey<OutputTuple> outKey = q.addStream("out", OutputTuple.class);

		q.addTextSource("inSource", args[1],
				new TextSourceFunction<InputTuple>() {
					@Override
					public InputTuple getNext(String line) {
						String[] tokens = line.split(",");
						return new InputTuple(Long.valueOf(tokens[0]), Integer
								.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
					}
				}, inKey);

		class Win extends BaseTimeBasedSingleWindow<InputTuple, OutputTuple> {

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
		;

		q.addAggregateOperator("aggOp", new Win(), 4 * 7 * 24 * 3600,
				7 * 24 * 3600, inKey, outKey);

		q.addTextSink("outSink", args[2], new TextSinkFunction<OutputTuple>() {
			@Override
			public String convertTupleToLine(OutputTuple tuple) {
				return tuple.timestamp + "," + tuple.key + "," + tuple.count
						+ "," + tuple.average;
			}
		}, outKey);

		q.activate();
		Util.sleep(30000);
		q.deActivate();

	}
}
