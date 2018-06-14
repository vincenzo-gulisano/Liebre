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

package query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class StatisticsConfiguration {

  private final String folder;
  private final boolean autoFlush;

  public StatisticsConfiguration(String folder, boolean autoFlush,
      StatisticType type) {
    this.folder = folder;
    this.autoFlush = autoFlush;
  }

  public String folder() {
    return folder;
  }

  public boolean autoFlush() {
    return autoFlush;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StatisticsConfiguration that = (StatisticsConfiguration) o;

    return new EqualsBuilder()
        .append(autoFlush, that.autoFlush)
        .append(folder, that.folder)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(folder)
        .append(autoFlush)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("folder", folder)
        .append("autoFlush", autoFlush)
        .appendSuper(super.toString())
        .toString();
  }




}
