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

import java.util.LinkedList;

import query.Query;
import sink.text.TextSinkFunction;
import source.text.TextSourceFunction;
import stream.StreamKey;
import tuple.Tuple;
import util.Util;

public class TextUnion {
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

		Query q = new Query();

		q.activateStatistics(args[0]);

		StreamKey<InputTuple> inKey1 = q.addStream("in1", InputTuple.class);
		StreamKey<InputTuple> inKey2 = q.addStream("in2", InputTuple.class);
		StreamKey<InputTuple> outKey = q.addStream("out", InputTuple.class);

		q.addTextSource("inSource1", args[1],
				new TextSourceFunction<InputTuple>() {
					@Override
					public InputTuple getNext(String line) {
						Util.sleep(50);
						String[] tokens = line.split(",");
						return new InputTuple(Long.valueOf(tokens[0]), Integer
								.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
					}
				}, inKey1);

		q.addTextSource("inSource2", args[2],
				new TextSourceFunction<InputTuple>() {
					@Override
					public InputTuple getNext(String line) {
						Util.sleep(50);
						String[] tokens = line.split(",");
						return new InputTuple(Long.valueOf(tokens[0]), Integer
								.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
					}
				}, inKey2);

		LinkedList<StreamKey<InputTuple>> ins = new LinkedList<StreamKey<InputTuple>>();
		ins.add(inKey1);
		ins.add(inKey2);
		q.addUnionOperator("union", ins, outKey);

		q.addTextSink("outSink", args[3], new TextSinkFunction<InputTuple>() {
			@Override
			public String convertTupleToLine(InputTuple tuple) {
				return tuple.timestamp + "," + tuple.key + "," + tuple.value;
			}
		}, outKey);

		q.activate();
		Util.sleep(40000);
		q.deActivate();

	}
}
