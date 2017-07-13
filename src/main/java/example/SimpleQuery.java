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

import java.util.Random;

import operator.BaseOperator;
import query.Query;
import sink.BaseSink;
import source.BaseSource;
import stream.StreamKey;
import tuple.Tuple;
import util.Util;

public class SimpleQuery {
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
		StreamKey<MyTuple> outKey = q.addStream("out", MyTuple.class);

		q.addSource("inSource", new BaseSource<MyTuple>() {
			Random r = new Random();

			@Override
			protected MyTuple getNextTuple() {
				Util.sleep(100);
				return new MyTuple(System.currentTimeMillis(), r.nextInt(5), r
						.nextInt(100));
			}
		}, inKey);

		q.addOperator("multiply", new BaseOperator<MyTuple, MyTuple>() {
			@Override
			protected void process() {
				MyTuple inTuple = in.getNextTuple();
				if (inTuple != null) {
					out.addTuple(new MyTuple(inTuple.timestamp, inTuple.key,
							inTuple.value * 2));
				}
			}
		}, inKey, outKey);

		q.addSink("outSink", new BaseSink<MyTuple>() {
			@Override
			protected void processTuple(MyTuple tuple) {
				System.out.println(tuple.timestamp + "," + tuple.key + ","
						+ tuple.value);
			}
		}, outKey);

		q.activate();
		Util.sleep(30000);
		q.deActivate();

	}
}
