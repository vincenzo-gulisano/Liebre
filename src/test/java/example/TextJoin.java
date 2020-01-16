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
import java.util.Random;

import common.tuple.BaseRichTuple;
import common.util.Util;
import component.operator.in2.Operator2In;
import component.operator.in2.join.JoinFunction;
import query.Query;
import component.sink.Sink;
import component.sink.SinkFunction;
import component.source.Source;
import component.source.SourceFunction;

public class TextJoin {

  public static void main(String[] args) {

    Guice.createInjector(new ExampleModule());

    Query q = new Query();

    Source<InputTuple1> i1 = q.addBaseSource("i1", new SourceFunction<InputTuple1>() {
      private final Random r = new Random();

      @Override
      public InputTuple1 get() {
        Util.sleep(1100);
        return new InputTuple1(System.currentTimeMillis(), r.nextInt(10));
      }
    });

    Source<InputTuple2> i2 = q.addBaseSource("i2", new SourceFunction<InputTuple2>() {
      private final Random r = new Random();

      @Override
      public InputTuple2 get() {
        Util.sleep(1100);
        return new InputTuple2(System.currentTimeMillis(), r.nextInt(20));
      }
    });

    Operator2In<InputTuple1, InputTuple2, OutputTuple> join = q.addJoinOperator("join",
        new JoinFunction<InputTuple1, InputTuple2, OutputTuple>() {
          @Override
          public OutputTuple apply(InputTuple1 t1, InputTuple2 t2) {
            if (t1.a < t2.b) {
              return new OutputTuple(t1.getTimestamp(), t1, t2);
            }
            return null;
          }
        }, 10000);

    Sink<OutputTuple> o1 = q.addBaseSink("o1", new SinkFunction<OutputTuple>() {
      @Override
      public void accept(OutputTuple tuple) {
        System.out.println(tuple.t1.a + " <--> " + tuple.t2.b);
      }
    });

    q.connect2inLeft(i1, join);
    q.connect2inRight(i2, join);
    q.connect(join, o1);

    q.activate();
    Util.sleep(30000);
    q.deActivate();

  }

  private static class InputTuple1 extends BaseRichTuple {

    public int a;

    public InputTuple1(long timestamp, int a) {
      super(timestamp, "");
      this.a = a;
    }

  }

  private static class InputTuple2 extends BaseRichTuple {

    public int b;

    public InputTuple2(long timestamp, int b) {
      super(timestamp, "");
      this.b = b;
    }

  }

  private static class OutputTuple extends BaseRichTuple {

    public InputTuple1 t1;
    public InputTuple2 t2;

    public OutputTuple(long timestamp, InputTuple1 t1, InputTuple2 t2) {
      super(timestamp, "");
      this.t1 = t1;
      this.t2 = t2;
    }

  }
}
