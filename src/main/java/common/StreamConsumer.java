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

package common;

import common.component.Component;
import common.tuple.Tuple;
import java.util.Collection;
import stream.Stream;

/**
 * A stream {@link Component} that consumes tuples.
 *
 * @param <IN> The input type for this component.
 */
public interface StreamConsumer<IN extends Tuple> extends Named, Component {

  void addInput(StreamProducer<IN> source, Stream<IN> stream);

  Stream<IN> getInput();

  <T extends Tuple> Collection<? extends Stream<T>> getInputs();

}
