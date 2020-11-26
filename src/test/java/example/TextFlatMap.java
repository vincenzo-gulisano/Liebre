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

import common.util.Util;
import component.operator.Operator;
import component.sink.Sink;
import component.source.Source;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import query.Query;

public class TextFlatMap {

  public static void main(String[] args) {

    final String reportFolder = args[0];
    final String inputFile = args[1];
    final String outputFile = reportFolder + File.separator + "TextFlatMap.out.csv";

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

    Operator<MyTuple, MyTuple> multiply = q
        .addFlatMapOperator("multiply", tuple -> {
          List<MyTuple> result = new LinkedList<MyTuple>();
          result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 2));
          result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 3));
          result.add(new MyTuple(tuple.timestamp, tuple.key, tuple.value * 4));
          return result;
        });

    Sink<MyTuple> o1 = q.addTextFileSink("o1", outputFile, true);

    q.connect(i1, inputReader).connect(inputReader, multiply).connect(multiply, o1);

    q.activate();
  }

}
