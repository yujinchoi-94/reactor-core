/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import reactor.util.concurrent.Queues;

/**
 * A wrapper around a {@link Sinks.Many}, with a similar API focusing on tryEmit methods
 * and allowing for treating onNext submissions as batches.
 * <p>
 * The goal of this class is to provide a Sink-like API that is less impacted in case of
 * heavy contention. Terminal signals ({@link #tryEmitComplete()} and {@link #tryEmitError(Throwable)})
 * must still be externally synchronized with {@link #trySubmitNext(Object)}, otherwise they
 * will return {@link reactor.core.publisher.Sinks.EmitResult#FAIL_NON_SERIALIZED}.
 * <p>
 * Parallel use of {@link #trySubmitNext(Object)} is detected and one winning caller will apply work-stealing
 * and process the data emitted by all other callers contending on the current batch.
 *
 * @author Simon Baslé
 */
public final class SinkManyBatchingMultiproducer<T> implements Sinks.SinksMultiproducer<T> {

	final Sinks.Many<T>       delegate;
	final Queue<T>            workQueue;
	final Consumer<? super T> clearConsumer;

	volatile     int                                                      state;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SinkManyBatchingMultiproducer> STATE =
		AtomicIntegerFieldUpdater.newUpdater(SinkManyBatchingMultiproducer.class, "state");

	static final int FLAG_CANCELLED = 0b10000000_00000000_00000000_00000000;
	static final int FLAG_TERMINATED = 0b01000000_00000000_00000000_00000000;
	static final int MASK_WIP = 0b00111111_11111111_11111111_11111111;
	static final int WIP_CANCELLED = -1;
	static final int WIP_TERMINATED = -2;

	public SinkManyBatchingMultiproducer(Sinks.Many<T> delegate, Consumer<? super T> clearConsumer) {
		this.delegate = delegate;
		this.clearConsumer = clearConsumer;
		this.workQueue = Queues.<T>unboundedMultiproducer().get();
	}

	//TODO document that now the only contention we care about is termination vs emit/drain
	//as soon as the submitted onNext have been processed, terminate will be taken into account

	@Override
	public Sinks.EmitResult tryEmitComplete() {
		if (state != 0) {
			return Sinks.EmitResult.FAIL_NON_SERIALIZED;
		}
		Sinks.EmitResult result = delegate.tryEmitComplete();
		if (result.isSuccess()) {
			if (markTerminated(this)) {
				clearRemaining();
			}
		}
		return result;
	}
	@Override
	public Sinks.EmitResult tryEmitError(Throwable t) {
		if (state != 0) {
			return Sinks.EmitResult.FAIL_NON_SERIALIZED;
		}
		Sinks.EmitResult result = delegate.tryEmitError(t);
		if (result.isSuccess()) {
			if (markTerminated(this)) {
				clearRemaining();
			}
		}
		return result;
	}

	@Override
	public int currentSubscriberCount() {
		return delegate.currentSubscriberCount();
	}

	@Override
	public Flux<T> asFlux() {
		return delegate.asFlux();
	}

	void clearRemaining() {
		T element = workQueue.poll();
		while (element != null) {
			this.clearConsumer.accept(element);
			element = workQueue.poll();
		}
	}

	@Override
	public Sinks.MultiproducerEmitResult<T> trySubmitNext(T value) {
		if (isTerminated(STATE.get(this))) {
			return new Sinks.MultiproducerEmitResult<>(Sinks.EmitResult.FAIL_TERMINATED, value);
		}
		else if (isCancelled(STATE.get(this))) {
			return new Sinks.MultiproducerEmitResult<>(Sinks.EmitResult.FAIL_CANCELLED, value);
		}

		int wip = getAndIncrementWip(this);
		if (wip == WIP_CANCELLED) {
			return new Sinks.MultiproducerEmitResult<>(Sinks.EmitResult.FAIL_CANCELLED, value);
		}
		else if (wip == WIP_TERMINATED) {
			return new Sinks.MultiproducerEmitResult<>(Sinks.EmitResult.FAIL_TERMINATED, value);
		}
		workQueue.add(value);
		if (wip == 0) {
			do {
				T polled = workQueue.poll();
				while (polled != null) {
					Sinks.EmitResult polledResult = delegate.tryEmitNext(polled);
					if (polledResult == Sinks.EmitResult.FAIL_CANCELLED) {
						if (markCancelled(this)) {
							clearRemaining();
						}
						return new Sinks.MultiproducerEmitResult<>(polledResult, polled);
					}
					else if (polledResult == Sinks.EmitResult.FAIL_TERMINATED) {
						if (markTerminated(this)) {
							clearRemaining();
						}
						return new Sinks.MultiproducerEmitResult<>(polledResult, polled);
					}
					else if (polledResult.isFailure()) {
						markNoWip(this);
						return new Sinks.MultiproducerEmitResult<>(polledResult, polled);
					}
					else {
						polled = workQueue.poll();
					}
				}
			}
			while (decrementAndGetWip(this) > 0);
			return new Sinks.MultiproducerEmitResult<>(Sinks.EmitResult.OK, null);
		}
		else {
			return new Sinks.MultiproducerEmitResult<>(Sinks.EmitResult.SUBMITTED_TO_BATCH, null);
		}
	}


	static boolean isCancelled(int state) {
		return (state & FLAG_CANCELLED) == FLAG_CANCELLED;
	}

	static boolean isTerminated(int state) {
		return (state & FLAG_TERMINATED) == FLAG_TERMINATED;
	}

	static boolean markCancelled(SinkManyBatchingMultiproducer<?> instance) {
		for(;;) {
			int state = STATE.get(instance);
			if (isCancelled(state) || isTerminated(state)) {
				return false;
			}
			if (STATE.compareAndSet(instance, state, state & FLAG_CANCELLED)) {
				return true;
			}
		}
	}

	static boolean markTerminated(SinkManyBatchingMultiproducer<?> instance) {
		for(;;) {
			int state = STATE.get(instance);
			if (isCancelled(state) || isTerminated(state)) {
				return false;
			}
			if (STATE.compareAndSet(instance, state, state & FLAG_TERMINATED)) {
				return true;
			}
		}
	}

	static boolean markNoWip(SinkManyBatchingMultiproducer<?> instance) {
		for(;;) {
			int state = STATE.get(instance);
			if (isCancelled(state) || isTerminated(state)) {
				return false;
			}
			if (STATE.compareAndSet(instance, state, state & ~MASK_WIP)) {
				return true;
			}
		}
	}

	static int getAndIncrementWip(SinkManyBatchingMultiproducer<?> instance) {
		for (;;) {
			int state = STATE.get(instance);
			if (isCancelled(state)) {
				return WIP_CANCELLED;
			}
			else if (isTerminated(state)) {
				return WIP_TERMINATED;
			}

			int wip = state & MASK_WIP; //at this point, we don't care about lowest bits since !cancelled/!terminated
			int newWip = wip;
			if (wip < MASK_WIP) {
				newWip = wip+1;
			}
			if (STATE.compareAndSet(instance, state, newWip)) {
				return wip;
			}
		}
	}

	static int decrementAndGetWip(SinkManyBatchingMultiproducer<?> instance) {
		for (;;) {
			int state = STATE.get(instance);
			int wip = state & MASK_WIP;
			//we need to preserve the non-WIP flags
			int flags = state & ~MASK_WIP;

			int newWip;
			int newState;

			if (flags != 0) {
				newWip = 0;
				newState = flags;
			}
			else {
				newWip = wip - 1;
				if (newWip < 0) {
					newWip = 0;
				}
				newState = flags | newWip;
			}

			if (STATE.compareAndSet(instance, state, newState)) {
				return newWip;
			}
		}
	}

}
