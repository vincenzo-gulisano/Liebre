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
import java.util.List;

import operator.router.RouterFunction;
import query.Query;
import sink.text.TextSinkFunction;
import source.text.TextSourceFunction;
import stream.StreamKey;
import tuple.BaseRichTuple;
import util.Util;

public class TextRouterMap {
	public static void main(String[] args) {

		class MyTuple extends BaseRichTuple {
			public int value;

			public MyTuple(long timestamp, String key, int value) {
				super(timestamp, key);
				this.value = value;
			}
		}

		Query q = new Query();

		q.activateStatistics(args[0]);

		StreamKey<MyTuple> inKey = q.addStream("in", MyTuple.class);
		StreamKey<MyTuple> outKey1 = q.addStream("out1", MyTuple.class);
		StreamKey<MyTuple> outKey2 = q.addStream("out2", MyTuple.class);

		q.addTextSource("inSource", args[1], new TextSourceFunction<MyTuple>() {
			@Override
			public MyTuple getNext(String line) {
				String[] tokens = line.split(",");
				return new MyTuple(Long.valueOf(tokens[0]), tokens[1], Integer
						.valueOf(tokens[2]));
			}
		}, inKey);

		LinkedList<StreamKey<MyTuple>> outs = new LinkedList<StreamKey<MyTuple>>();
		outs.add(outKey1);
		outs.add(outKey2);
		q.addRouterOperator("router", new RouterFunction<MyTuple>() {

			@Override
			public List<String> chooseStreams(MyTuple tuple) {
				List<String> result = new LinkedList<String>();
				if (tuple.getKey().equals("28")) {
					result.add("out1");
				} else if (tuple.getKey().equals("29")) {
					result.add("out2");
				}
				return result;
			}
		}, inKey, outs);

		q.addTextSink("outSink1", args[2], new TextSinkFunction<MyTuple>() {
			@Override
			public String convertTupleToLine(MyTuple tuple) {
				return tuple.getTimestamp() + "," + tuple.getKey() + ","
						+ tuple.value;
			}
		}, outKey1);

		q.addTextSink("outSink2", args[3], new TextSinkFunction<MyTuple>() {
			@Override
			public String convertTupleToLine(MyTuple tuple) {
				return tuple.getTimestamp() + "," + tuple.getKey() + ","
						+ tuple.value;
			}
		}, outKey2);

		q.activate();
		Util.sleep(5000);
		q.deActivate();

	}
}
