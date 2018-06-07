/*  Copyright (C) 2017-2018
 *  Vincenzo Gulisano
 *  Dimitris Palyvos Giannas
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
 *  Contact:
 *    Vincenzo Gulisano info@vincenzogulisano.com
 *    Dimitris Palyvos Giannas palyvos@chalmers.se
 */

package common.component;

/**
 * Type of event processed in {@link Component#wait(EventType)} and Component{@link
 * #notify(ComponentState)}. Takes care of the actual delegation to the correct functions in {@link
 * ComponentState}.
 *
 * @author palivosd
 */
public enum EventType {
  READ {
    @Override
    protected void setValue(ComponentState state, boolean value) {
      state.setCanRead(value);
    }
  },
  WRITE {
    @Override
    protected void setValue(ComponentState state, boolean value) {
      state.setCanWrite(value);
    }
  };

  protected abstract void setValue(ComponentState state, boolean value);

  /**
   * Set the given {@link ComponentState} to wait for this event type
   */
  public void wait(ComponentState state) {
    setValue(state, false);
  }

  /**
   * Notify {@link ComponentState} to wait for this event type
   */
  public void notify(ComponentState state) {
    setValue(state, true);
  }
}
