package common.exec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import common.ActiveRunnable;
import common.NamedEntity;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

//FIXME: Builder
public class BoxState<IN extends Tuple, OUT extends Tuple> {

	public static enum BoxType {
		OPERATOR {
			@Override
			protected boolean checkState(BoxState<?, ?> state) {
				return state.inputs.size() == 1 && state.next.size() == 1;
			}
		},
		OPERATOR2IN {

			@Override
			protected boolean checkState(BoxState<?, ?> state) {
				return state.inputs.size() == 2 && state.next.size() == 1;
			}
		},
		UNION {
			@Override
			protected boolean checkState(BoxState<?, ?> state) {
				return state.inputs.size() >= 1 && state.next.size() == 1;
			}
		},
		ROUTER {
			@Override
			protected boolean checkState(BoxState<?, ?> state) {
				return state.inputs.size() == 1 && state.next.size() >= 1;
			}
		},
		SOURCE {
			@Override
			protected boolean checkState(BoxState<?, ?> state) {
				return state.inputs.isEmpty() && state.next.size() == 1;
			}
		},
		SINK {
			@Override
			protected boolean checkState(BoxState<?, ?> state) {
				return state.inputs.size() == 1 && state.next.isEmpty();
			}
		};
		protected abstract boolean checkState(BoxState<?, ?> state);
	}

	private static AtomicInteger nextIndex = new AtomicInteger();
	private final String id;
	private final int index;
	private final StreamFactory factory;
	private volatile boolean enabled;

	private final Map<String, Stream<IN>> inputs = new ConcurrentHashMap<>();
	private final Map<String, StreamProducer<? extends Tuple>> previous = new ConcurrentHashMap<>();
	private final Map<String, StreamConsumer<OUT>> next = new ConcurrentHashMap<>();

	private final BoxType type;

	public BoxState(String id, BoxType type, StreamFactory streamFactory) {
		this.id = id;
		this.index = nextIndex.getAndIncrement();
		this.type = type;
		this.factory = streamFactory;
	}

	public void enable() {
		if (!type.checkState(this)) {
			throw new IllegalStateException(id);
		}
		for (Stream<?> input : inputs.values()) {
			input.enable();
		}
		this.enabled = true;
	}

	public boolean isEnabled() {
		return this.enabled;
	}

	public void disable() {
		for (Stream<?> input : inputs.values()) {
			input.disable();
		}
		this.enabled = false;
	}

	public String getId() {
		return id;
	}

	public int getIndex() {
		return index;
	}

	public void setOutput(String key, StreamConsumer<OUT> out, StreamProducer<OUT> caller) {
		Object previousOutput = next.putIfAbsent(key, out);
		if (previousOutput != null) {
			throw new IllegalArgumentException(String.format("Output %s is already set to %s!", key, previousOutput));
		}

		out.registerIn(caller);
	}

	public void setInput(String key, StreamProducer<IN> in, ActiveRunnable caller) {
		if (factory == null) {
			throw new IllegalStateException("This entity cannot have inputs. Factory == null");
		}
		if (enabled) {
			throw new IllegalStateException("Cannot register input while running");
		}
		if (!nextIsSet(in, caller)) {
			System.err.println(
					"WARNING: It seems that you are explicitly registering inputs. Please use addOutput() instead!");
		}
		Object previousInput = inputs.putIfAbsent(key, factory.newStream(in, caller));
		if (previousInput != null) {
			throw new IllegalArgumentException(String.format("Input %s is already set to %s!", key, previousInput));
		}
		previous.put(key, in);
	}

	private boolean nextIsSet(StreamProducer<IN> prev, NamedEntity current) {
		List<String> nextIds = new ArrayList<>();
		for (NamedEntity next : prev.getNext()) {
			nextIds.add(next.getId());
		}
		return nextIds.contains(current.getId());
	}

	public Stream<IN> getInputStream(String key) {
		return inputs.get(key);
	}

	public Collection<StreamConsumer<OUT>> getNext() {
		return next.values();
	}

	public Stream<OUT> getOutputStream(String destId, StreamProducer<OUT> src) {
		// The IDs of both ends of the stream are needed in case we have a router ->
		// union/join connection
		return next.get(destId).getInputStream(src.getId());
	}

	public StreamFactory getStreamFactory() {
		return this.factory;
	}

	public Collection<Stream<IN>> getInputs() {
		return inputs.values();
	}

	public Collection<StreamProducer<? extends Tuple>> getPrevious() {
		return previous.values();
	}

	public boolean hasInput() {
		for (Stream<?> in : inputs.values()) {
			if (in.peek() == null) {
				return false;
			}
		}
		return true;
	}

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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof BoxState))
			return false;
		BoxState<?, ?> other = (BoxState<?, ?>) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "BoxState [id=" + id + ", enabled=" + enabled + ", inputs=" + inputs + ", previous=" + previous
				+ ", next=" + next + ", type=" + type + "]";
	}

}
