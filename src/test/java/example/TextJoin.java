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

import common.tuple.BaseRichTuple;
import common.util.Util;
import operator.in2.join.Predicate;
import query.Query;
import sink.BaseSink;
import source.BaseSource;
import stream.StreamKey;

public class TextJoin {
	public static void main(String[] args) {

		class InputTuple1 extends BaseRichTuple {

			public int a;

			public InputTuple1(long timestamp, int a) {
				super(timestamp, "");
				this.a = a;
			}

		}

		class InputTuple2 extends BaseRichTuple {

			public int b;

			public InputTuple2(long timestamp, int b) {
				super(timestamp, "");
				this.b = b;
			}

		}

		class OutputTuple extends BaseRichTuple {

			public InputTuple1 t1;
			public InputTuple2 t2;

			public OutputTuple(long timestamp, InputTuple1 t1, InputTuple2 t2) {
				super(timestamp, "");
				this.t1 = t1;
				this.t2 = t2;
			}

		}

		Query q = new Query();

		q.activateStatistics(args[0]);

		StreamKey<InputTuple1> in1Key = q.addStream("in1", InputTuple1.class);
		StreamKey<InputTuple2> in2Key = q.addStream("in2", InputTuple2.class);
		StreamKey<OutputTuple> outKey = q.addStream("out", OutputTuple.class);

		q.addSource("inSource1", new BaseSource<InputTuple1>() {
			Random r = new Random();

			@Override
			public InputTuple1 getNextTuple() {
				try {
					Thread.sleep(1100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return new InputTuple1(System.currentTimeMillis(), r
						.nextInt(10));
			}
		}, in1Key);

		q.addSource("inSource2", new BaseSource<InputTuple2>() {
			Random r = new Random();

			@Override
			public InputTuple2 getNextTuple() {
				try {
					Thread.sleep(800);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				return new InputTuple2(System.currentTimeMillis(), r
						.nextInt(20));
			}
		}, in2Key);

		q.addJoinOperator("join",
				new Predicate<InputTuple1, InputTuple2, OutputTuple>() {
					@Override
					public OutputTuple compare(InputTuple1 t1, InputTuple2 t2) {
						if (t1.a < t2.b)
							return new OutputTuple(t1.getTimestamp(), t1, t2);
						return null;
					}
				}, 10000, in1Key, in2Key, outKey);

		q.addSink("outSink", new BaseSink<OutputTuple>() {
			@Override
			public void processTuple(OutputTuple tuple) {
				System.out.println(tuple.t1.a + " <--> " + tuple.t2.b);
			}
		}, outKey);

		q.activate();
		Util.sleep(30000);
		q.deActivate();

	}
}
