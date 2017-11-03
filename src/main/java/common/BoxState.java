package common;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import common.tuple.Tuple;
import stream.Stream;
import stream.StreamFactory;

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

	private final String id;
	private final StreamFactory factory;
	private volatile boolean enabled;

	private final Map<String, Stream<IN>> inputs = new ConcurrentHashMap<>();
	private final List<StreamProducer<? extends Tuple>> previous = new CopyOnWriteArrayList<>();
	private final Map<String, StreamConsumer<OUT>> next = new ConcurrentHashMap<>();

	private final BoxType type;

	public BoxState(String id, BoxType type, StreamFactory streamFactory) {
		this.id = id;
		this.type = type;
		this.factory = streamFactory;
	}

	public void enable() {
		if (!type.checkState(this)) {
			throw new IllegalStateException(id);
		}
		this.enabled = true;

	}

	public boolean isEnabled() {
		return this.enabled;
	}

	public void disable() {
		this.enabled = false;
	}

	public String getId() {
		return id;
	}

	public void setInput(String key, StreamProducer<IN> in, Object caller) {
		if (factory == null) {
			throw new IllegalStateException("This entity cannot have inputs. Factory == null");
		}
		if (enabled) {
			throw new IllegalStateException("Cannot register input while running");
		}
		if (!in.getNext().contains(caller)) {
			throw new UnsupportedOperationException("Please use registerOut() to construct query graphs");
		}
		inputs.put(key, factory.newStream(in.getId(), id));
		previous.add(in);
	}

	public Stream<IN> getInputStream(String key) {
		return inputs.get(key);
	}

	public void setOutput(String key, StreamConsumer<OUT> out, StreamProducer<OUT> caller) {
		next.put(key, out);
		out.registerIn(caller);
	}

	public Collection<StreamConsumer<OUT>> getNext() {
		return next.values();
	}

	public Stream<OUT> getOutputStream(String key, StreamProducer<OUT> caller) {
		// Both IDs needed in case we have a router -> union connection
		return next.get(key).getInputStream(caller.getId());
	}

	public StreamFactory getStreamFactory() {
		return this.factory;
	}

	public Collection<Stream<IN>> getInputs() {
		return inputs.values();
	}

	public Collection<StreamProducer<? extends Tuple>> getPrevious() {
		return previous;
	}

}
