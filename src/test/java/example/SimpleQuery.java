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
import java.util.Random;

import common.BoxState.BoxType;
import common.tuple.Tuple;
import common.util.Util;
import operator.BaseOperator;
import operator.Operator;
import query.ConcurrentLinkedListStreamFactory;
import query.Query;
import sink.Sink;
import sink.SinkFunction;
import source.Source;
import source.SourceFunction;

public class SimpleQuery {
	private static class MyTuple implements Tuple {
		public long timestamp;
		public int key;
		public int value;

		public MyTuple(long timestamp, int key, int value) {
			this.timestamp = timestamp;
			this.key = key;
			this.value = value;
		}
	}

	public static void main(String[] args) {
		final String reportFolder = args[0];

		Query q = new Query();
		q.activateStatistics(reportFolder);
		Source<MyTuple> source = q.addBaseSource("I1", new SourceFunction<MyTuple>() {
			private final Random r = new Random();

			@Override
			public MyTuple getNextTuple() {
				Util.sleep(50);
				return new MyTuple(System.currentTimeMillis(), r.nextInt(5), r.nextInt(100));
			}
		});

		Operator<MyTuple, MyTuple> multiply = q.addOperator(
				new BaseOperator<MyTuple, MyTuple>("M", BoxType.OPERATOR, ConcurrentLinkedListStreamFactory.INSTANCE) {
					@Override
					public List<MyTuple> processTuple(MyTuple tuple) {
						List<MyTuple> result = new LinkedList<MyTuple>();
						result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2));
						return result;
					}
				});

		Sink<MyTuple> sink = q.addBaseSink("O1", new SinkFunction<MyTuple>() {

			@Override
			public void processTuple(MyTuple tuple) {
				System.out.println(tuple.timestamp + "," + tuple.key + "," + tuple.value);
			}
		});
		source.addOutput(multiply);
		multiply.addOutput(sink);

		q.activate();
		Util.sleep(30000);
		q.deActivate();

	}
}
