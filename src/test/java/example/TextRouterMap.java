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
import component.operator.Operator;
import component.sink.Sink;
import component.source.Source;
import common.util.Util;
import java.io.File;
import query.Query;

public class TextRouterMap {

  public static void main(String[] args) {

    final String reportFolder = args[0];
    final String inputFile = args[1];
    final String outputFile1 = reportFolder + File.separator + "TextRouterMap_Out1.out.csv";
    final String outputFile2 = reportFolder + File.separator + "TextRouterMap_Out2.out.csv";

    Guice.createInjector(new ExampleModule());
    Query q = new Query();

    Source<String> i1 = q.addTextFileSource("I1", inputFile);

    Operator<String, MyTuple> inputReader =
        q.addMapOperator(
            "map",
            line -> {
              Util.sleep(100);
              String[] tokens = line.split(",");
              return new MyTuple(
                  Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
            });

    Operator<MyTuple, MyTuple> router = q.addRouterOperator("router");

    Operator<MyTuple, MyTuple> filterHigh =
        q.addFilterOperator("fHigh", t -> Integer.valueOf(t.getKey()) < 5);

    Operator<MyTuple, MyTuple> filterLow =
        q.addFilterOperator("fLow", t -> Integer.valueOf(t.getKey()) > 4);

    Sink<MyTuple> o1 = q.addTextFileSink("o1", outputFile1, true);
    Sink<MyTuple> o2 = q.addTextFileSink("o2", outputFile2, true);

    q.connect(i1, inputReader)
        .connect(inputReader, router)
        .connect(router, filterHigh)
        .connect(filterHigh, o1)
        .connect(router, filterLow)
        .connect(filterLow, o2);

    q.activate();
    Util.sleep(5000);
    q.deActivate();
  }
}
