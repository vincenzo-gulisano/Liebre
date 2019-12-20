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

import io.palyvos.dcs.common.util.Util;
import component.operator.Operator;
import component.operator.in1.filter.FilterFunction;
import component.operator.in1.map.MapFunction;
import component.sink.Sink;
import component.source.Source;
import java.io.File;
import query.Query;

public class TextMapFilter {

  public static void main(String[] args) {
    final String reportFolder = args[0];
    final String inputFile = args[1];
    final String outputFile = reportFolder + File.separator + "TextMapFilter.out.csv";
    Query q = new Query();

    Source<MyTuple> i1 = q.addTextFileSource("I1", inputFile, line -> {
      Util.sleep(15);
      String[] tokens = line.split(",");
      return new MyTuple(Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]),
          Integer.valueOf(tokens[2]));
    });

    Operator<MyTuple, MyTuple> multiply = q
        .addMapOperator("multiply", new MapFunction<MyTuple, MyTuple>() {
          @Override
          public MyTuple apply(MyTuple tuple) {
            return new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2);
          }
        });

    Operator<MyTuple, MyTuple> filter = q
        .addFilterOperator("filter", new FilterFunction<MyTuple>() {

          @Override
          public boolean test(MyTuple tuple) {
            return tuple.value >= 150;
          }

        });

    Sink<MyTuple> o1 = q.addTextFileSink("o1", outputFile, tuple -> {
      return tuple.timestamp + "," + tuple.key + "," + tuple.value;
    });

    q.connect(i1, multiply).connect(multiply, filter).connect(filter, o1);

    q.activate();
    Util.sleep(20000);
    q.deActivate();

  }

  private static class MyTuple {

    public long timestamp;
    public int key;
    public int value;

    public MyTuple(long timestamp, int key, int value) {
      this.timestamp = timestamp;
      this.key = key;
      this.value = value;
    }
  }
}
