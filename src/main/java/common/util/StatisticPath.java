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

package common.util;

import common.Named;
import java.io.File;
import scheduling.thread.LiebreThread;

/**
 * Helper class that forces a specific structure for the names of the files used by various
 * entities. That structure is
 * <pre>
 *   folderPath/filename.type.csv
 * </pre>
 * where the variables {@code folderPath, filename, type} can be either manually provided.
 * <br />
 * If a {@link Named} object is provided or a {@link LiebreThread}, then the {@code filename} is
 * automatically set to {@link Named#getId()} or {@link LiebreThread#getId()} respectively.
 * <br />
 * If a specific constant of the {@link StatisticType }enum is used to generate the statistic,
 * then the {@code type} is
 * inferred to be the constant's name in lowercase. For example {@code StatisticPath.get
 * ("f1", op1, StatisticType.IN} would generate the statistic path {@code f1/OP1_ID.in.csv}, where
 * "OP1_ID" is the
 * id of op1.
 */
public final class StatisticPath {

  private StatisticPath() {

  }

  public static String get(String folder, Named entity, String type) {
    String name = entity.getId();
    return get(folder, name, type);
  }

  public static String get(String folder, Named entity, StatisticType type) {
    String name = entity.getId();
    return get(folder, name, type.name().toLowerCase());
  }

  public static String get(String folder, LiebreThread thread, String type) {
    String name = String.valueOf(thread.getId());
    return get(folder, name, type);
  }

  public static String get(String folder, LiebreThread thread, StatisticType type) {
    String name = String.valueOf(thread.getId());
    return get(folder, name, type.name().toLowerCase());
  }

  /**
   * Get the filename of the statistic. The general structure
   */
  public static String get(String folder, String filename, String type) {
    StringBuilder sb = new StringBuilder();
    sb.append(folder);
    sb.append(File.separator);
    sb.append(filename);
    sb.append(".").append(type);
    sb.append(".csv");
    return sb.toString();
  }

}
