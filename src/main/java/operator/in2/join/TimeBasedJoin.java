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

package operator.in2.join;

import java.util.LinkedList;
import java.util.List;

import common.tuple.RichTuple;
import operator.in2.BaseOperator2In;
import stream.StreamFactory;

public class TimeBasedJoin<IN extends RichTuple, IN2 extends RichTuple, OUT extends RichTuple>
    extends BaseOperator2In<IN, IN2, OUT> {

  JoinFunction<IN, IN2, OUT> joinFunction;
  private long ws;
  private LinkedList<IN> in1Tuples;
  private LinkedList<IN2> in2Tuples;
  // This is for determinism
  private LinkedList<IN> in1TuplesBuffer;
  private LinkedList<IN2> in2TuplesBuffer;

  public TimeBasedJoin(String id, StreamFactory streamFactory, long windowSize,
      JoinFunction<IN, IN2, OUT> joinFunction) {
    super(id);
    this.ws = windowSize;
    this.joinFunction = joinFunction;

    in1Tuples = new LinkedList<IN>();
    in2Tuples = new LinkedList<IN2>();

    in1TuplesBuffer = new LinkedList<IN>();
    in2TuplesBuffer = new LinkedList<IN2>();
  }

  protected void purge(long ts) {
    while (in1Tuples.size() > 0 && in1Tuples.peek().getTimestamp() < ts - ws) {
      in1Tuples.poll();
    }
    while (in2Tuples.size() > 0 && in2Tuples.peek().getTimestamp() < ts - ws) {
      in2Tuples.poll();
    }
  }

  private List<OUT> processReadyTuples() {

    List<OUT> results = new LinkedList<OUT>();

    while (in1buffered() && in2buffered()) {
      if (buffer1Peek().getTimestamp() < buffer2Peek().getTimestamp()) {

        IN tuple = buffer1Poll();

        purge(tuple.getTimestamp());

        if (in2Tuples.size() > 0) {

          for (IN2 t : in2Tuples) {
            OUT result = joinFunction.apply(tuple, t);
            if (result != null) {
              results.add(result);
            }

          }

        }

        in1Tuples.add(tuple);

      } else {

        IN2 tuple = buffer2Poll();

        purge(tuple.getTimestamp());

        if (in1Tuples.size() > 0) {

          for (IN t : in1Tuples) {
            OUT result = joinFunction.apply(t, tuple);
            if (result != null) {
              results.add(result);
            }

          }

        }

        in2Tuples.add(tuple);

      }
    }

    return results;

  }

  @Override
  public List<OUT> processTupleIn1(IN tuple) {

    in1buffer(tuple);
    return processReadyTuples();

  }

  @Override
  public List<OUT> processTupleIn2(IN2 tuple) {

    in2buffer(tuple);
    return processReadyTuples();

  }

  private boolean in1buffered() {
    return !in1TuplesBuffer.isEmpty();
  }

  private boolean in2buffered() {
    return !in2TuplesBuffer.isEmpty();
  }

  private void in1buffer(IN t) {
    in1TuplesBuffer.add(t);
  }

  private void in2buffer(IN2 t) {
    in2TuplesBuffer.add(t);
  }

  private IN buffer1Peek() {
    return in1TuplesBuffer.peek();
  }

  private IN2 buffer2Peek() {
    return in2TuplesBuffer.peek();
  }

  private IN buffer1Poll() {
    return in1TuplesBuffer.poll();
  }

  private IN2 buffer2Poll() {
    return in2TuplesBuffer.poll();
  }

}
