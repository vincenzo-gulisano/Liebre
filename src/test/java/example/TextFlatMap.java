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

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import common.tuple.Tuple;
import common.util.Util;
import operator.Operator;
import operator.map.FlatMapFunction;
import query.Query;
import sink.Sink;
import sink.TextSinkFunction;
import source.Source;
import source.TextSourceFunction;

public class TextFlatMap {
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
		final String inputFile = args[1];
		final String outputFile = reportFolder + File.separator + "TextFlatMap.out.csv";

		Query q = new Query();

		q.activateStatistics(reportFolder);
		Source<MyTuple> i1 = q.addBaseSource("I1", new TextSourceFunction<MyTuple>(inputFile) {

			@Override
			protected MyTuple getNext(String line) {
				Util.sleep(100);
				String[] tokens = line.split(",");
				return new MyTuple(Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
			}
		});

		;

		Operator<MyTuple, MyTuple> multiply = q.addMapOperator("multiply", new FlatMapFunction<MyTuple, MyTuple>() {
			@Override
			public List<MyTuple> map(MyTuple tuple) {
				List<MyTuple> result = new LinkedList<MyTuple>();
				result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2));
				result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 3));
				result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 4));
				return result;
			}
		});

		Sink<MyTuple> o1 = q.addBaseSink("o1", new TextSinkFunction<MyTuple>(outputFile) {

			@Override
			public String processTupleToText(MyTuple tuple) {
				return tuple.timestamp + "," + tuple.key + "," + tuple.value;
			}

		});

		i1.addOutput(multiply);
		multiply.addOutput(o1);

		q.activate();
		Util.sleep(30000);
		q.deActivate();

	}

}
