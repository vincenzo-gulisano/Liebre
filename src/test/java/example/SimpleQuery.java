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

import common.tuple.Tuple;
import common.util.Util;
import component.operator.Operator;
import component.operator.in1.BaseOperator1In;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import query.Query;

public class SimpleQuery {

  public static void main(String[] args) {
    final String reportFolder = args[0];

    Query q = new Query();
    q.activateStatistics(reportFolder);
    Source<MyTuple> source = q.addBaseSource("I1", new SourceFunction<MyTuple>() {
      private final Random r = new Random();

      @Override
      public MyTuple get() {
        Util.sleep(50);
        return new MyTuple(System.currentTimeMillis(), r.nextInt(5), r.nextInt(100));
      }
    });

    Operator<MyTuple, MyTuple> multiply = q
        .addOperator(new BaseOperator1In<MyTuple, MyTuple>("M",0,0) {
          @Override
          public List<MyTuple> processTupleIn1(MyTuple tuple) {
            List<MyTuple> result = new LinkedList<MyTuple>();
            result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2));
            return result;
          }
        });

    Sink<MyTuple> sink = q.addBaseSink("O1",
        tuple -> System.out.println(tuple.timestamp + "," + tuple.key + "," + tuple.value));

    q.connect(source, multiply).connect(multiply, sink);

    q.activate();
    Util.sleep(30000);
    q.deActivate();

  }

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
}
