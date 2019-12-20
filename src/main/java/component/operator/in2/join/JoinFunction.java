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

package component.operator.in2.join;

import component.ComponentFunction;
import java.util.function.BiFunction;

/**
 * Function that is applied to two tuples (with possibly different data types) and generates a new
 * tuple or null.
 *
 * @param <IN1> The input type of the first tuple.
 * @param <IN2> The input type of the second tuple.
 * @param <OUT> The resulting tuple type.
 */
public interface JoinFunction<IN1, IN2, OUT> extends
    ComponentFunction, BiFunction<IN1, IN2, OUT> {


}
