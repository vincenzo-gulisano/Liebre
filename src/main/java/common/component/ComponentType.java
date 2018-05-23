/*  Copyright (C) 2017-2018  Vincenzo Gulisano, Dimitris Palyvos Giannas
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

import org.apache.commons.lang3.Validate;

/**
 * Enum representing the various types of components which are available as separate subclasses.
 * Defines several functions that check for the invariants of the states of each component typel
 *
 * @author palivosd
 */
public enum ComponentType {
  OPERATOR(ConnectionsNumber.ONE, ConnectionsNumber.ONE),
  OPERATOR2IN(ConnectionsNumber.TWO, ConnectionsNumber.ONE),
  UNION(ConnectionsNumber.N, ConnectionsNumber.ONE),
  ROUTER(ConnectionsNumber.ONE, ConnectionsNumber.N),
  SOURCE(ConnectionsNumber.NONE, ConnectionsNumber.ONE),
  SINK(ConnectionsNumber.ONE, ConnectionsNumber.NONE);

  private final ConnectionsNumber inputsNumber;
  private final ConnectionsNumber outputsNumber;

  ComponentType(ConnectionsNumber inputsNumber,
      ConnectionsNumber outputsNumber) {
    this.inputsNumber = inputsNumber;
    this.outputsNumber = outputsNumber;
  }

  public void validateInputs(ComponentState<?, ?> state) {
    int size = state.getInputs().size();
    Validate.validState(inputsNumber.isValid(size), "Invalid inputs number for component '%s': %d", state.getId(), size);
  }

  public void validateOutputs(ComponentState<?, ?> state) {
    int size = state.getOutputs().size();
    Validate.validState(outputsNumber.isValid(size), "Invalid outputs number for component '%s': %d", state.getId(), size);
  }

  public void validate(ComponentState<?, ?> state) {
    validateInputs(state);
    validateOutputs(state);
  }

  public boolean isProducer() {
    return outputsNumber != ConnectionsNumber.NONE;
  }

  public boolean isConsumer() {
    return inputsNumber != ConnectionsNumber.NONE;
  }

  private static enum ConnectionsNumber {
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
  }
}
