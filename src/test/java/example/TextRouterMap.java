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

import common.tuple.BaseRichTuple;
import common.util.Util;
import java.io.File;
import operator.Operator;
import query.Query;
import sink.Sink;
import source.Source;

public class TextRouterMap {

  public static void main(String[] args) {

    final String reportFolder = args[0];
    final String inputFile = args[1];
    final String outputFile1 = reportFolder + File.separator + "TextRouterMap_Out1.out.csv";
    final String outputFile2 = reportFolder + File.separator + "TextRouterMap_Out2.out.csv";
    Query q = new Query();

    q.activateStatistics(reportFolder);
    Source<MyTuple> i1 = q.addTextFileSource("I1", inputFile, line -> {
      Util.sleep(15);
      String[] tokens = line.split(",");
      return new MyTuple(Long.valueOf(tokens[0]), tokens[1], Integer.valueOf(tokens[2]));
    });

    Operator<MyTuple, MyTuple> router = q.addRouterOperator("router");

    Operator<MyTuple, MyTuple> filterHigh = q
        .addFilterOperator("fHigh", t -> Integer.valueOf(t.getKey()) < 5);

    Operator<MyTuple, MyTuple> filterLow = q
        .addFilterOperator("fLow", t -> Integer.valueOf(t.getKey()) > 4);

    Sink<MyTuple> o1 = q.addTextFileSink("o1", outputFile1, tuple -> {
      return tuple.getTimestamp() + "," + tuple.getKey() + "," + tuple.value;
    });
    Sink<MyTuple> o2 = q.addTextFileSink("o2", outputFile2, tuple -> {
      return tuple.getTimestamp() + "," + tuple.getKey() + "," + tuple.value;
    });

    q.connect(i1, router)
        .connect(router, filterHigh).connect(filterHigh, o1)
        .connect(router, filterLow).connect(filterLow, o2);

    q.activate();
    Util.sleep(5000);
    q.deActivate();

  }

  private static class MyTuple extends BaseRichTuple {

    public int value;

    public MyTuple(long timestamp, String key, int value) {
      super(timestamp, key);
      this.value = value;
    }
  }
}
