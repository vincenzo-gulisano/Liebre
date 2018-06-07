/*  Copyright (C) 2017  Vincenzo Gulisano
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package example;

import common.tuple.Tuple;
import common.util.Util;
import common.util.backoff.NoopBackoff;
import java.io.File;
import java.util.concurrent.TimeUnit;
import operator.Operator;
import query.Query;
import scheduling.Scheduler;
import scheduling.impl.ProbabilisticTaskPoolStatistic;
import scheduling.impl.ThreadPoolScheduler;
import scheduling.priority.PriorityMetricFactory;
import sink.Sink;
import source.Source;

public class TextUnion {

  public static void main(String[] args) {

    final String reportFolder = args[0];
    final String inputFile1 = args[1];
    final String inputFile2 = args[2];
    final String outputFile = reportFolder + File.separator + "TextUnion.out.csv";

    Scheduler scheduler = new ThreadPoolScheduler(2, 100, TimeUnit.MILLISECONDS,
        new ProbabilisticTaskPoolStatistic(PriorityMetricFactory.QUEUE_SIZE, 1, 1000000, reportFolder))
        .enableSourceThreads();

    Query q = new Query(scheduler);

    q.activateStatistics(reportFolder);

    Source<MyTuple> i1 = q.addTextFileSource("i1", inputFile1, line -> {
      Util.sleep(5);
      String[] tokens = line.split(",");
      return new MyTuple(Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]),
          Integer.valueOf(tokens[2]));
    });

    Source<MyTuple> i2 = q.addTextFileSource("i2", inputFile2, line -> {
      Util.sleep(1);
      String[] tokens = line.split(",");
      return new MyTuple(Long.valueOf(tokens[0]), Integer.valueOf(tokens[1]),
          Integer.valueOf(tokens[2]));
    });

    Operator<MyTuple, MyTuple> union = q.addUnionOperator("union");

    Sink<MyTuple> o1 = q.addTextFileSink("o1", outputFile, tuple -> {
      Util.sleep(100);
      return tuple.timestamp + "," + tuple.key + "," + tuple.value;
    });

    q.connect(i1, union);
    q.connect(i2, union, NoopBackoff.INSTANCE);
    q.connect(union, o1);

    q.activate();
    Util.sleep(40000);
    q.deActivate();

  }

  private static class MyTuple implements Tuple {

    public long timestamp;
    public int key;
    public int value;

    public MyTuple(long timestamp, int key, int value) {
      this.timestamp = timestamp;
      this.key = key;
      this.value = value;
    }
  }
}
