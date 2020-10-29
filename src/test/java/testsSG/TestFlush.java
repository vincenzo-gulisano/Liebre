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

package testsSG;

import common.tuple.BaseRichTuple;
import common.util.Util;
import component.operator.Operator;
import component.operator.in1.BaseOperator1In;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;
import query.Query;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class TestFlush {


    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        Query q = new Query();
        // q.activateStatistics(reportFolder);
        Source<MyTuple> source1 = q.addBaseSource("S1",
                new SourceFunction<MyTuple>() {
                    private final Random r = new Random();
                    private long ts = 0;

                    @Override
                    public MyTuple get() {
                        Util.sleep(1000);
                        MyTuple t = new MyTuple(ts, "",
                                "S1", r.nextInt(100));
                        System.out.println("Sending " + t);
                        ts++;
                        return t;
                    }
                });
        Source<MyTuple> source2 = q.addBaseSource("S2",
                new SourceFunction<MyTuple>() {
                    private final Random r = new Random();
                    private long ts = 0;

                    @Override
                    public MyTuple get() {
                        Util.sleep(1000);
                        MyTuple t = new MyTuple(ts, "",
                                "S2", r.nextInt(100));
                        System.out.println("Sending " + t);
                        ts++;
                        return t;
                    }
                });

        Operator<MyTuple, MyTuple> multiply = q
                .addOperator(new BaseOperator1In<MyTuple, MyTuple>("M") {

                    long lastTimestamp = -1;

                    @Override
                    public List<MyTuple> processTupleIn1(MyTuple tuple) {
                        assert lastTimestamp == -1 || (tuple.getTimestamp() >= lastTimestamp);
                        lastTimestamp = tuple.getTimestamp();
                        List<MyTuple> result = new LinkedList<MyTuple>();
                        result.add(new MyTuple(tuple.getTimestamp(), tuple
                                .getKey(), tuple.source, tuple.value * 2));
                        return result;
                    }
                });

        Sink<MyTuple> sink = q.addBaseSink("O1",
                tuple -> System.out.println("Received " + tuple));

        q.connect(Arrays.asList(source1, source2), Arrays.asList(multiply))
                .connect(multiply, sink);

        q.activate();
        System.out.println("Sleeping 10 seconds");
        Util.sleep(3000);
        System.out.println("Disabling sources");
        source1.disable();
        source2.disable();
        System.out.println("Sleeping 10 seconds");
        Util.sleep(10000);
        System.out.println("Flushing ScaleGate and sleeping a bit more ");
        Util.sleep(1000);
        multiply.getInput().flush();
        Util.sleep(3000);
        q.deActivate();

    }

    private static class MyTuple extends BaseRichTuple implements Comparable<BaseRichTuple> {

        public String source;
        public int value;

        public MyTuple(long timestamp, String key, String source, int value) {
            super(timestamp, key);
            this.source = source;
            this.value = value;
        }

        @Override
        public String toString() {
            return getTimestamp() + "," + getKey() + "," + source + "," + value;
        }
    }
}
