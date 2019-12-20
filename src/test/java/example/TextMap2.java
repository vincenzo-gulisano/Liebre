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
import io.palyvos.dcs.common.util.Util;
import java.io.File;
import query.Query;

public class TextMap2 {

  public static void main(String[] args) {
    final String reportFolder = args[0];
    final String inputFile = args[1];
    final String outputFile = reportFolder + File.separator + "TextMap2.out.csv";

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

    Operator<MyTuple, OutputTuple> transform =
        q.addMapOperator("transform", tuple -> new OutputTuple(tuple));

    Sink<OutputTuple> o1 = q.addTextFileSink("o1", outputFile, true);

    q.connect(i1, inputReader).connect(inputReader, transform).connect(transform, o1);

    q.activate();
    Util.sleep(40000);
    q.deActivate();
  }

  private static class OutputTuple {

    public long timestamp;
    public int key;
    public int valueA;
    public int valueB;
    public int valueC;

    public OutputTuple(MyTuple t) {
      this.timestamp = t.timestamp;
      this.key = t.key;
      this.valueA = t.value * 2;
      this.valueB = t.value / 2;
      this.valueC = t.value + 10;
    }
  }
}
