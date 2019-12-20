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

import io.palyvos.liebre.common.util.Util;
import component.operator.Operator;
import component.operator.in1.map.MapFunction;
import component.sink.Sink;
import component.source.Source;
import java.io.File;
import query.Query;

public class TextMap2 {

  public static void main(String[] args) {
    final String reportFolder = args[0];
    final String inputFile = args[1];
    final String outputFile = reportFolder + File.separator + "TextMap2.out.csv";
    Query q = new Query();

    Source<InputTuple> i1 = q.addTextFileSource("I1", inputFile, line -> {
      Util.sleep(100);
      String[] tokens = line.split(",");
      return new InputTuple(Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]),
          Integer.valueOf(tokens[2]));
    });

    Operator<InputTuple, OutputTuple> transform = q.addMapOperator("transform",
        new MapFunction<InputTuple, OutputTuple>() {
          @Override
          public OutputTuple apply(InputTuple tuple) {
            return new OutputTuple(tuple);
          }
        });

    Sink<OutputTuple> o1 = q.addTextFileSink("o1", outputFile, tuple -> {
      return tuple.timestamp + "," + tuple.key + "," + tuple.valueA + "," + tuple.valueB + ","
          + tuple.valueC;
    });

    q.connect(i1, transform).connect(transform, o1);

    q.activate();
    Util.sleep(40000);
    q.deActivate();

  }

  private static class InputTuple {

    public long timestamp;
    public int key;
    public int value;

    public InputTuple(long timestamp, int key, int value) {
      this.timestamp = timestamp;
      this.key = key;
      this.value = value;
    }
  }

  private static class OutputTuple {

    public long timestamp;
    public int key;
    public int valueA;
    public int valueB;
    public int valueC;

    public OutputTuple(InputTuple t) {
      this.timestamp = t.timestamp;
      this.key = t.key;
      this.valueA = t.value * 2;
      this.valueB = t.value / 2;
      this.valueC = t.value + 10;
    }
  }
}
