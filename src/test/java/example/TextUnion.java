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

import common.tuple.Tuple;
import common.util.Util;
import operator.Operator;
import query.Query;
import sink.Sink;
import sink.TextSinkFunction;
import source.Source;
import source.TextSourceFunction;

public class TextUnion {

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
		final String inputFile1 = args[1];
		final String inputFile2 = args[2];
		final String outputFile = reportFolder + File.separator + "TextUnion.out.csv";

		Query q = new Query();

		q.activateStatistics(reportFolder);

		Source<MyTuple> i1 = q.addBaseSource("i1", new TextSourceFunction<MyTuple>(inputFile1) {

			@Override
			protected MyTuple getNext(String line) {
				Util.sleep(50);
				String[] tokens = line.split(",");
				return new MyTuple(Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
			}
		});

		Source<MyTuple> i2 = q.addBaseSource("i1", new TextSourceFunction<MyTuple>(inputFile2) {

			@Override
			protected MyTuple getNext(String line) {
				Util.sleep(50);
				String[] tokens = line.split(",");
				return new MyTuple(Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
			}
		});

		Operator<MyTuple, MyTuple> union = q.addUnionOperator("union");

		Sink<MyTuple> o1 = q.addBaseSink("o1", new TextSinkFunction<MyTuple>(outputFile) {

			@Override
			protected String processTupleToText(MyTuple tuple) {
				return tuple.timestamp + "," + tuple.key + "," + tuple.value;

			}
		});

		i1.addOutput(union);
		i2.addOutput(union);
		union.addOutput(o1);

		q.activate();
		Util.sleep(40000);
		q.deActivate();

	}
}
