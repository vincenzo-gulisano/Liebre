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

package component;

/**
 * Representation of the (abstract) amount of connections that a component can have. Can be used to
 * differentiate behavior when having operators with single or multiple connections respectively.
 *
 * @author palivosd
 */
public enum ConnectionsNumber {
  NONE {
    @Override
    protected boolean isValid(int number) {
      return number == 0;
    }
  },
  ONE {
    @Override
    protected boolean isValid(int number) {
      return number == 1;
    }
  },
  TWO {
    @Override
    protected boolean isValid(int number) {
      return number == 2;
    }
  },
  N {
    @Override
    protected boolean isValid(int number) {
      return number >= 1;
    }
  };

  protected abstract boolean isValid(int number);

  /**
   * Check if this member represents multiple connections
   * @return {@code true} if connections > 1
   */
  public boolean isMultiple() {
    return this == TWO || this == N;
  }

  /**
   * Check if this member represents just one connection just one connection.
   * @return {@code true} if connections == 1
   */
  public boolean isSingle() {
    return this == ONE;
  }
}
