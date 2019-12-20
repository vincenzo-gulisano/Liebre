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

package component.operator.in1.filter;

import component.operator.in1.BaseOperator1In;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.Validate;

/**
 * Operator implementation that applies {@link FilterFunction}s to streams. A {@link FilterOperator}
 * decides whether tuples should be forwarded further or not in the stream by applying a boolean
 * predicate to each one of them.
 *
 * @param <T> The type of the tuples.
 */
public class FilterOperator<T> extends BaseOperator1In<T, T> {

  protected FilterFunction<T> filter;

  public FilterOperator(String id, FilterFunction<T> filter) {
    super(id);
    Validate.notNull(filter, "filter");
    this.filter = filter;
  }

  @Override
  public void enable() {
    filter.enable();
    super.enable();
  }

  @Override
  public void disable() {
    super.disable();
    filter.disable();
  }

  @Override
  public List<T> processTupleIn1(T tuple) {
    List<T> result = new LinkedList<T>();
    if (filter.test(tuple)) {
      result.add(tuple);
    }
    return result;
  }

  @Override
  public boolean canRun() {
    return filter.canRun() && super.canRun();
  }
}
