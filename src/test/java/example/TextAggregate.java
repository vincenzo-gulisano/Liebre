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
import component.operator.Operator;
import component.operator.in1.aggregate.AggregateType;
import component.operator.in1.aggregate.BaseTimeBasedSingleWindow;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.sink.Sink;
import component.source.Source;
import query.Query;

public class TextAggregate {

  public static void main(String[] args) {
    final String reportFolder = args[0];
    final String inputFile = args[1];
    final String outputFile = args[2];
    final long WINDOW_SIZE = 100;
    final long WINDOW_SLIDE = 20;


    Query q = new Query();

    Source<String> i1 = q.addTextFileSource("I1", inputFile);

    Operator<String, InputTuple> inputReader =
        q.addMapOperator(
            "map",
            line -> {
              String[] tokens = line.split(",");
              return new InputTuple(
                  Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]), Integer.valueOf(tokens[2]));
            });

    Operator<InputTuple, OutputTuple> aggregate =
        q.addAggregateOperator("aggOp", new AverageWindow(), WINDOW_SIZE, WINDOW_SLIDE, AggregateType.MULTIWINDOW);

    Sink<OutputTuple> o1 = q.addTextFileSink("o1", outputFile, true);

    q.connect(i1, inputReader).connect(inputReader, aggregate).connect(aggregate, o1);

    q.activate();
    Util.sleep(30000);
    q.deActivate();
  }

  private static class InputTuple extends BaseRichTuple {

    public int value;

    public InputTuple(long timestamp, int key, int value) {
      super(timestamp, key + "");
      this.value = value;
    }
  }

  private static class OutputTuple extends BaseRichTuple {

    public int count;
    public double average;

    public OutputTuple(long timestamp, int key, int count, double average) {
      super(timestamp, key + "");
      this.count = count;
      this.average = average;
    }

    @Override
    public String toString() {
      return timestamp + "," + key + "," + count +
              "," + average;
    }
  }

  private static class AverageWindow extends BaseTimeBasedSingleWindow<InputTuple, OutputTuple> {

    private double count = 0;
    private double sum = 0;

    @Override
    public void add(InputTuple t) {
      count++;
      sum += t.value;
    }

    @Override
    public void remove(InputTuple t) {
      count--;
      sum -= t.value;
    }

    @Override
    public OutputTuple getAggregatedResult() {
      double average = count > 0 ? sum / count : 0;
      return new OutputTuple(startTimestamp, Integer.valueOf(key), (int) count, average);
    }

    @Override
    public TimeWindowAddRemove<InputTuple, OutputTuple> factory() {
      return new AverageWindow();
    }
  }
}
