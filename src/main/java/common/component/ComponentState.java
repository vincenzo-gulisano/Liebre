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

import common.Named;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import stream.Stream;
import stream.StreamFactory;

/**
 * Object that represents the state of all common stream components such as operators, sinks and
 * sources.
 *
 * @param <IN> The type of the inputs of the component where the state belongs to.
 * @param <OUT> The type of the outputs of the component where the state belongs to.
 * @author palivosd
 */
public class ComponentState<IN extends Tuple, OUT extends Tuple> {

  private static AtomicInteger nextIndex = new AtomicInteger();
  private final ComponentType type;
  private final String id;
  private final int index;
  private final Map<String, Stream<IN>> inputs = new HashMap<>();
  private final Map<String, StreamProducer<? extends Tuple>> previous = new HashMap<>();
  private final Map<String, StreamConsumer<OUT>> next = new HashMap<>();
  private final StreamFactory factory;

  private volatile boolean enabled = false;

  /**
   * FIXME: Global factory for streams <br>
   * Construct
   * @param id The unique ID of the component
   * @param type The type of the component
   * @param factory The factory that will be used to generate Streams for this component
   */
  public ComponentState(String id, ComponentType type, StreamFactory factory) {
    Validate.notBlank(id);
    Validate.notNull(type);
    this.id = id;
    this.factory = factory;
    this.type = type;
    this.index = nextIndex.getAndIncrement();
  }

  /**
   * Associate a downstream operator with a given output key. Will automatically do the reverse
   * association by calling {@link #setInput(String, StreamProducer, Component)} at the downstream
   * operator.
   *
   * @param key The output key
   * @param out The downstream operator
   * @param caller The operator that will write to this output
   * @throws IllegalStateException If the same key was already used before
   */
  public void setOutput(String key, StreamConsumer<OUT> out, StreamProducer<OUT> caller) {
    Object previousOutput = next.putIfAbsent(key, out);
    Validate
        .validState(previousOutput == null, "Output %s is already set to %s!", key, previousOutput);
    out.registerIn(caller);
  }

  /**
   * Create an input stream and associate it with a unique key and a specific writer. <b>Must never
   * be called explicitly! Use {@link #setOutput(String, StreamConsumer, StreamProducer)}
   * instead!</b>
   *
   * @param key The unique key that will be used to recall this stream
   * @param in The writer that will be writing to this stream
   * @param caller The caller, meaning the reader of this stream
   * @throws IllegalStateException if the key was already used or if it was not called through
   * {@link #setOutput(String, StreamConsumer, StreamProducer)}
   */
  public void setInput(String key, StreamProducer<IN> in, Component caller) {
    Validate.validState(factory != null, "Stream factory in component '%s' is null!", id);
    Validate.validState(nextIsSet(in, caller),
        "It seems that setInput() was invoked explicitly by '%s'. Please use addOutput() instead!",
        in.getId());
    Object previousInput = inputs.putIfAbsent(key, factory.newStream(in, caller));
    Validate.validState(previousInput == null, "Input '%s' is already set to '%s'!", key,
        previousInput);
    previous.put(key, in);
  }

  /**
   * Helper function to determine whether {@link #setInput(String, StreamProducer, Component)} is
   * called explicitly or implicitly
   */
  private boolean nextIsSet(StreamProducer<IN> prev, Named current) {
    List<String> nextIds = new ArrayList<>();
    for (Named next : prev.getNext()) {
      nextIds.add(next.getId());
    }
    return nextIds.contains(current.getId());
  }


  /**
   * Enable the state. Should always be called when calling {@link Component#enable()}
   */
  public void enable() {
    Validate
        .isTrue(type.isStateValid(this), "State of '%s' is not valid for components of type '%s'",
            id, type);
    for (Stream<?> input : inputs.values()) {
      input.enable();
    }
    this.enabled = true;
  }

  /**
   * @return {@code true} if the state is enabled
   */
  public boolean isEnabled() {
    return this.enabled;
  }

  /**
   * Disable the state. Should always be called when calling {@link Component#disable()}
   */
  public void disable() {
    for (Stream<?> input : inputs.values()) {
      input.disable();
    }
    this.enabled = false;
  }

  /**
   * Get the unique ID of the state.
   *
   * @return The unique ID of the state.
   */
  public String getId() {
    return id;
  }

  /**
   * Get the unique numerical ID of the state. This can be the same or different than {@link
   * #getId()}.
   *
   * @return The unique numerical index of the state.
   */
  public int getIndex() {
    return index;
  }

  /**
   * Get the input stream of the state that has the provided key
   *
   * @param key The key of the input stream.
   * @return The stream with the given key.
   */
  public Stream<IN> getInputStream(String key) {
    Stream<IN> stream = inputs.get(key);
    Validate
        .validState(stream != null, "Component '%s' does not have a stream with key '%s'", id, key);
    return stream;
  }

  /**
   * Get all the "next" {@link StreamConsumer} components, that are directly downstream of the
   * component where this state belongs to in the stream graph.
   *
   * @return The {@link StreamConsumer}s.
   */
  public Collection<StreamConsumer<OUT>> getNext() {
    return next.values();
  }

  /**
   * FIXME: No need to pass producer as a parameter Get the output stream that connects the
   * component that this state belongs to with the downstream omponent with the given ID.
   *
   * @param destId The downstream component that is connected to the output.
   * @param src The component that this state belongs to.
   * @return The unique output stream that connects the two components.
   */
  public Stream<OUT> getOutputStream(String destId, StreamProducer<OUT> src) {
    StreamConsumer<OUT> nextComponent = next.get(destId);
    Validate.validState(nextComponent != null,
        "Component '%s' does not have component '%s' as a direct descendant", id, destId);
    // The IDs of both ends of the stream are needed in case we have a router ->
    // union/join connection
    Stream<OUT> stream = nextComponent.getInputStream(src.getId());
    Validate.validState(stream != null, "Component '%s' does not have an input to component '%s'",
        nextComponent.getId(), src.getId());
    return stream;
  }

  /**
   * Get all the input streams of this state.
   *
   * @return The input streams.
   */
  public Collection<Stream<IN>> getInputs() {
    return inputs.values();
  }

  /**
   * Get all the upstream components of the component where this state belongs to
   *
   * @return The upstream {@link StreamProducer} components.
   */
  public Collection<StreamProducer<? extends Tuple>> getPrevious() {
    return previous.values();
  }

  /**
   * Verify that this state has input streams with {@link Tuple}s which can be read.
   *
   * @return {@code true} if all input streams are not empty.
   */
  public boolean hasInput() {
    for (Stream<?> in : inputs.values()) {
      if (in.peek() == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Verify that this state has output streams with non-zero capacity.
   *
   * @return {@code true} if all output streams have non-zero capacity.
   */
  public boolean hasOutput() {
    for (StreamConsumer<OUT> out : next.values()) {
      Stream<OUT> output = out.getInputStream(getId());
      if (output.remainingCapacity() == 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ComponentState<?, ?> that = (ComponentState<?, ?>) o;

    return new EqualsBuilder()
        .append(index, that.index)
        .append(id, that.id)
        .append(type, that.type)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(id)
        .append(index)
        .append(type)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("id", id)
        .append("index", index)
        .append("inputs", inputs)
        .append("previous", previous)
        .append("next", next)
        .append("type", type)
        .append("enabled", enabled)
        .toString();
  }
}
