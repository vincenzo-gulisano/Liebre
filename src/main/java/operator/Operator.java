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

package operator;

import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;

/**
 * An operator is a processing unit that both consumes and produces tuples.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public interface Operator<IN extends Tuple, OUT extends Tuple> extends StreamConsumer<IN>,
    StreamProducer<OUT> {

}
