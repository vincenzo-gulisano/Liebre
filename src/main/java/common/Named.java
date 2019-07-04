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


package common;

/**
 * Interface that marks entities that have a unique ID and numerical index.
 *
 * @author palivosd
 */
public interface Named {

  /**
   * Get the unique ID of the entity.
   *
   * @return The unique ID of the entity.
   */
  String getId();

  /**
   * Get the unique numerical index of the entity.
   *
   * @return The unique numerical index of the entity.
   */
  int getIndex();
  
  /**
   * 
   * Get the relative index (starting in 0) of this entity with respect to a connected downstream entity
   * 
   * @param index, the index of the entity of which this entity is a producer
   * @return The relative index of this entity as producer of its connected downstream entity
   */
  int getRelativeProducerIndex(int index);
  
  /**
   * 
   * Get the relative index (starting in 0) of this entity with respect to a connected upstream entity
   * 
   * @param index, the index of the entity of which this entity is a consumer
   * @return The relative index of this entity as consumer of its connected upstream entity
   */
  int getRelativeConsumerIndex(int index);
  
}
