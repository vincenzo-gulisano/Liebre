/*
 * Copyright (C) 2017-2018
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

package scheduling.priority;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.Component;
import common.tuple.Tuple;
import java.util.List;
import operator.Operator;
import sink.Sink;
import source.Source;
import stream.Stream;

public class QueueSizeMetric extends PriorityMetric {

  // TODO: Optimization where we record which boxes are "dirty" and update only
  // these priorities
  public QueueSizeMetric(List<Component> tasks, List<Component> ignoredTasks) {
    super(tasks, ignoredTasks);
  }

  @Override
  public <IN extends Tuple> void recordTupleRead(IN tuple, Stream<IN> input) {
  }

  @Override
  public <OUT extends Tuple> void recordTupleWrite(OUT tuple, Stream<OUT> output) {
  }

  @Override
  public List<Double> getPriorities(int scaleFactor) {
    long[] priorities = new long[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      priorities[i] = getPriority(tasks.get(i));
    }
    return scale(priorities, scaleFactor);
  }

  private int getPriority(Component task) {
    if (isIgnored(task)) {
      return 0;
    }
    if (task instanceof Source) {
      return getOutputCapacity((Source<?>) task);
    } else if (task instanceof Sink) {
      return getInputSize((Sink<?>) task);
    } else if (task instanceof Operator<?, ?>) {
      Operator<?, ?> operator = (Operator<?, ?>) task;
      return Math.min(getInputSize(operator), getOutputCapacity(operator));
    } else {
      throw new IllegalArgumentException("Cannot produce metric for class: " + task.getClass());
    }
  }

  private <IN extends Tuple> int getInputSize(StreamConsumer<IN> consumer) {
    int size = 0;
    for (Stream<?> input : consumer.getInputs()) {
      size += input.size();
    }
    return size;
  }

  private <OUT extends Tuple> int getOutputCapacity(StreamProducer<OUT> producer) {
    int minCapacity = -1;
    for (Stream<?> output : producer.getOutputs()) {
      int outputCapacity = output.remainingCapacity();
      minCapacity =
          minCapacity >= 0 ? Math.min(minCapacity, outputCapacity) : output.remainingCapacity();
    }
    return Math.max(1, minCapacity);
  }

}
