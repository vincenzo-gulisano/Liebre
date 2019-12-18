/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package example;

import com.google.inject.Guice;
import com.google.inject.Injector;
import common.tuple.BaseRichTuple;
import component.operator.Operator;
import component.operator.in1.BaseOperator1In;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;
import io.palyvos.liebre.common.util.Util;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import query.Query;

public class SGStreamExample {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		// final String reportFolder = args[0];

		Query q = new Query();
		Injector injector = Guice.createInjector(new ExampleModule());
		q.activateStatistics("test");
		// q.activateStatistics(reportFolder);
		Source<MyTuple> source1 = q.addBaseSource("S1",
				new SourceFunction<MyTuple>() {
					private final Random r = new Random();

					@Override
					public MyTuple get() {
						Util.sleep(25 + r.nextInt(25));
						return new MyTuple(System.currentTimeMillis(), "",
								"S1", r.nextInt(100));
					}
				});
		Source<MyTuple> source2 = q.addBaseSource("S2",
				new SourceFunction<MyTuple>() {
					private final Random r = new Random();

					@Override
					public MyTuple get() {
						Util.sleep(25 + r.nextInt(25));
						return new MyTuple(System.currentTimeMillis(), "",
								"S2", r.nextInt(100));
					}
				});

		Operator<MyTuple, MyTuple> multiply = q
				.addOperator(new BaseOperator1In<MyTuple, MyTuple>("M", 0, 0) {

					long lastTimestamp = -1;

					@Override
					public List<MyTuple> processTupleIn1(MyTuple tuple) {
						if (!(lastTimestamp == -1)) {
							assert (tuple.getTimestamp() >= lastTimestamp);
						}
						lastTimestamp = tuple.getTimestamp();
						List<MyTuple> result = new LinkedList<MyTuple>();
						result.add(new MyTuple(tuple.getTimestamp(), tuple
								.getKey(), tuple.source, tuple.value * 2));
						return result;
					}
				});

		Sink<MyTuple> sink = q.addBaseSink("O1",
				tuple -> System.out.println(tuple));

		q.connect(Arrays.asList(source1, source2),  Arrays.asList(multiply))
				.connect(multiply, sink);

		q.activate();
		Util.sleep(30000);
		q.deActivate();

	}

	private static class MyTuple extends BaseRichTuple implements Comparable<BaseRichTuple> {

		public String source;
		public int value;

		public MyTuple(long timestamp, String key, String source, int value) {
			super(timestamp, key);
			this.source = source;
			this.value = value;
		}

		@Override
		public String toString() {
			return getTimestamp() + "," + getKey() + "," + source + "," + value;
		}
	}
}
