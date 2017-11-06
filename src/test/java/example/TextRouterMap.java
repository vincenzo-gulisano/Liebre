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

import common.tuple.BaseRichTuple;
import common.util.Util;
import operator.Operator;
import operator.router.RouterFunction;
import query.Query;
import sink.Sink;
import sink.TextSinkFunction;
import source.Source;
import source.TextSourceFunction;

public class TextRouterMap {
	private static class MyTuple extends BaseRichTuple {
		public int value;

		public MyTuple(long timestamp, String key, int value) {
			super(timestamp, key);
			this.value = value;
		}
	}

	public static void main(String[] args) {

		final String reportFolder = args[0];
		final String inputFile = args[1];
		final String outputFile1 = reportFolder + File.separator + "TextRouterMap_Out1.out.csv";
		final String outputFile2 = reportFolder + File.separator + "TextRouterMap_Out2.out.csv";
		Query q = new Query();

		q.activateStatistics(reportFolder);
		Source<MyTuple> i1 = q.addBaseSource("I1", new TextSourceFunction<MyTuple>(inputFile) {

			@Override
			protected MyTuple getNext(String line) {
				Util.sleep(15);
				String[] tokens = line.split(",");
				return new MyTuple(Long.valueOf(tokens[0]), tokens[1], Integer.valueOf(tokens[2]));
			}
		});

		Operator<MyTuple, MyTuple> router = q.addRouterOperator("router", new RouterFunction<MyTuple>() {

			@Override
			public List<String> chooseOperators(MyTuple tuple) {
				List<String> result = new LinkedList<String>();
				int key = Integer.valueOf(tuple.getKey());
				if (key < 5) {
					result.add("o1");
				}
				if (key > 4) {
					result.add("o2");
				}
				return result;
			}
		});

		Sink<MyTuple> o1 = q.addBaseSink("o1", new TextSinkFunction<MyTuple>(outputFile1) {

			@Override
			public String processTupleToText(MyTuple tuple) {
				return tuple.getTimestamp() + "," + tuple.getKey() + "," + tuple.value;
			}
		});
		Sink<MyTuple> o2 = q.addBaseSink("o2", new TextSinkFunction<MyTuple>(outputFile2) {

			@Override
			public String processTupleToText(MyTuple tuple) {
				return tuple.getTimestamp() + "," + tuple.getKey() + "," + tuple.value;
			}
		});

		i1.registerOut(router);
		router.registerOut(o1);
		router.registerOut(o2);

		q.activate();
		Util.sleep(5000);
		q.deActivate();

	}
}
