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

package common.tuple;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BaseRichTuple implements RichTuple {

  protected final long timestamp;
  protected final long stimulus;
  protected final String key;

  public BaseRichTuple(long stimulus, long timestamp, String key) {
    this.timestamp = timestamp;
    this.stimulus = stimulus;
    this.key = key;
  }

  public BaseRichTuple(long timestamp, String key) {
    this(timestamp, System.currentTimeMillis(), key);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public long getStimulus() {
    return stimulus;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseRichTuple that = (BaseRichTuple) o;

    return new EqualsBuilder()
        .append(timestamp, that.timestamp)
        .append(stimulus, that.stimulus)
        .append(key, that.key)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(timestamp)
        .append(stimulus)
        .append(key)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("timestamp", timestamp)
        .append("stimulus", stimulus)
        .append("key", key)
        .appendSuper(super.toString())
        .toString();
  }


}
