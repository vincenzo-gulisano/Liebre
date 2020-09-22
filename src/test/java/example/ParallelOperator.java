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
import component.operator.in1.BaseOperator1In;
import component.operator.in1.map.MapFunction;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;
import query.Query;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class ParallelOperator {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        Query q = new Query();

        Source<MyTuple> source1 = q.addBaseSource("S1",
                new SourceFunction<MyTuple>() {
                    private final Random r = new Random();

                    @Override
                    public MyTuple get() {
                        Util.sleep(1000 + r.nextInt(1000));
                        return new MyTuple(System.currentTimeMillis(), "",
                                "S1", r.nextInt(100));
                    }
                });
        Source<MyTuple> source2 = q.addBaseSource("S2",
                new SourceFunction<MyTuple>() {
                    private final Random r = new Random();

                    @Override
                    public MyTuple get() {
                        Util.sleep(1000 + r.nextInt(1000));
                        return new MyTuple(System.currentTimeMillis(), "",
                                "S2", r.nextInt(100));
                    }
                });

        List<Operator<MyTuple, MyTuple>> maps = q.addMapOperator("map", new MapFunction<MyTuple, MyTuple>() {
            @Override
            public MyTuple apply(MyTuple myTuple) {
                System.out.println(myTuple);
                return null;
            }
        }, 3);

        Sink<MyTuple> sink = q.addBaseSink("O1",
                tuple -> System.out.println(tuple));

        q.connect(Arrays.asList(source1, source2), maps).connect(maps, Arrays.asList(sink));

        q.activate();
        Util.sleep(30000);
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
