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

package component.operator.router;

import common.tuple.Tuple;
import java.util.Collection;
import component.operator.Operator;
import stream.SWSRStream;

/**
 * {@link Operator} that copies an input tuple to multiple output streams.
 *
 * @param <T> The type of input/output tuples.
 */
public interface RouterOperator<T extends Tuple> extends Operator<T, T> {

  Collection<? extends SWSRStream<T>> chooseOutputs(T tuple);

}
