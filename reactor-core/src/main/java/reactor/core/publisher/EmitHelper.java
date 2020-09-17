/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

import org.reactivestreams.Subscriber;

import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.publisher.Sinks.Emission.FAIL_NON_SERIALIZED;

/**
 * @author Simon Baslé
 */
public interface EmitHelper {

	static EmitHelper retryOnConcurrentAccess() {
		return DefaultEmitHelper.FULL_BUSY_LOOPING;
	}

	static EmitHelper retryOnConcurrentAccess(boolean onlyBusyLoopForTerminalSignals) {
		return onlyBusyLoopForTerminalSignals ? DefaultEmitHelper.TERMINAL_BUSY_LOOPING  :
				DefaultEmitHelper.FULL_BUSY_LOOPING;
	}

	static EmitHelper failFast() {
		return DefaultEmitHelper.NO_BUSY_LOOPING;
	}

	static EmitHelper unsafeThrowing() {
		return ThrowingEmitHelper.INSTANCE;
	}

	/**
	 * Terminate the sequence successfully, generating an {@link Subscriber#onComplete() onComplete}
	 * signal.
	 * <p>
	 * Generally, {@link #tryEmitComplete()} is preferable, since it allows a custom handling
	 * of error cases.
	 *
	 * @implNote Implementors should typically delegate to {@link #tryEmitComplete()}. Failure {@link Emission}
	 * don't need any particular handling where emitComplete is concerned.
	 *
	 * @see #tryEmitComplete()
	 * @see Subscriber#onComplete()
	 */
	void emitComplete(Sinks.Many<?> sink);

	/**
	 * Terminate the sequence successfully, generating an {@link Subscriber#onComplete() onComplete}
	 * signal.
	 * <p>
	 * Generally, {@link #tryEmitEmpty()} is preferable, since it allows a custom handling
	 * of error cases.
	 *
	 * @implNote Implementors should typically delegate to {@link #tryEmitEmpty()}. Failure {@link Emission}
	 * don't need any particular handling where emitEmpty is concerned.
	 *
	 * @see #tryEmitEmpty()
	 * @see Subscriber#onComplete()
	 */
	void emitEmpty(Sinks.Empty<?> sink);

	/**
	 * Fail the sequence, generating an {@link Subscriber#onError(Throwable) onError}
	 * signal.
	 * <p>
	 * Generally, {@link #tryEmitError(Throwable)} is preferable since it allows a custom handling
	 * of error cases, although this implies checking the returned {@link Emission} and correctly
	 * acting on it (see implementation notes).
	 *
	 * @implNote Implementors should typically delegate to {@link #tryEmitError(Throwable)} and act on
	 * {@link Emission#FAIL_TERMINATED} by calling {@link Operators#onErrorDropped(Throwable, Context)}.
	 *
	 * @param error the exception to signal, not null
	 * @see #tryEmitError(Throwable)
	 * @see Subscriber#onError(Throwable)
	 */
	void emitError(Sinks.Many<?> sink, @Nullable Throwable error);

	/**
	 * Fail the sequence, generating an {@link Subscriber#onError(Throwable) onError}
	 * signal.
	 * <p>
	 * Generally, {@link #tryEmitError(Throwable)} is preferable since it allows a custom handling
	 * of error cases, although this implies checking the returned {@link Emission} and correctly
	 * acting on it (see implementation notes).
	 *
	 * @implNote Implementors should typically delegate to {@link #tryEmitError(Throwable)} and act on
	 * {@link Emission#FAIL_TERMINATED} by calling {@link Operators#onErrorDropped(Throwable, Context)}.
	 *
	 * @param error the exception to signal, not null
	 * @see #tryEmitError(Throwable)
	 * @see Subscriber#onError(Throwable)
	 */
	void emitError(Sinks.Empty<?> sink, @Nullable Throwable error);

	/**
	 * Emit a non-null element, generating an {@link Subscriber#onNext(Object) onNext} signal,
	 * or notifies the downstream subscriber(s) of a failure to do so via {@link #emitError(Throwable)}
	 * (with an {@link Exceptions#isOverflow(Throwable) overflow exception}).
	 * <p>
	 * Generally, {@link #tryEmitNext(Object)} is preferable since it allows a custom handling
	 * of error cases, although this implies checking the returned {@link Emission} and correctly
	 * acting on it (see implementation notes).
	 * <p>
	 * Might throw an unchecked exception in case of a fatal error downstream which cannot
	 * be propagated to any asynchronous handler (aka a bubbling exception).
	 *
	 * @implNote Implementors should typically delegate to {@link #tryEmitNext(Object)} and act on
	 * failures: {@link Emission#FAIL_OVERFLOW} should lead to {@link Operators#onDiscard(Object, Context)} followed
	 * by {@link #emitError(Throwable)}. {@link Emission#FAIL_CANCELLED} should lead to {@link Operators#onDiscard(Object, Context)}.
	 * {@link Emission#FAIL_TERMINATED} should lead to {@link Operators#onNextDropped(Object, Context)}.
	 *
	 * @param t the value to emit, not null
	 * @see #tryEmitNext(Object)
	 * @see Subscriber#onNext(Object)
	 */
	<T> void emitNext(Sinks.Many<T> sink, T v);

	/**
	 * Emit a non-null element, generating an {@link Subscriber#onNext(Object) onNext} signal
	 * immediately followed by an {@link Subscriber#onComplete() onComplete} signal,
	 * or notifies the downstream subscriber(s) of a failure to do so via {@link #emitError(Throwable)}
	 * (with an {@link Exceptions#isOverflow(Throwable) overflow exception}).
	 * <p>
	 * Generally, {@link #tryEmitValue(Object)} is preferable since it allows a custom handling
	 * of error cases, although this implies checking the returned {@link Emission} and correctly
	 * acting on it (see implementation notes).
	 * <p>
	 * Might throw an unchecked exception in case of a fatal error downstream which cannot
	 * be propagated to any asynchronous handler (aka a bubbling exception).
	 *
	 * @implNote Implementors should typically delegate to {@link #tryEmitValue (Object)} and act on
	 * failures: {@link Emission#FAIL_OVERFLOW} should lead to {@link Operators#onDiscard(Object, Context)} followed
	 * by {@link #emitError(Throwable)}. {@link Emission#FAIL_CANCELLED} should lead to {@link Operators#onDiscard(Object, Context)}.
	 * {@link Emission#FAIL_TERMINATED} should lead to {@link Operators#onNextDropped(Object, Context)}.
	 *
	 * @param value the value to emit and complete with, or {@code null} to only trigger an onComplete
	 * @see #tryEmitValue(Object)
	 * @see Subscriber#onNext(Object)
	 * @see Subscriber#onComplete()
	 */
	<T> void emitValue(Sinks.One<T> sink, @Nullable T v);
}

final class DefaultEmitHelper implements EmitHelper {

	static final long TEN_SECONDS_IN_LOOPS_OF_TEN_NANOS = Duration.ofSeconds(10).toNanos() / 10L;

	static final DefaultEmitHelper FULL_BUSY_LOOPING = new DefaultEmitHelper(true, true,  10L, TEN_SECONDS_IN_LOOPS_OF_TEN_NANOS);
	static final DefaultEmitHelper TERMINAL_BUSY_LOOPING = new DefaultEmitHelper(false, true,  10L, TEN_SECONDS_IN_LOOPS_OF_TEN_NANOS);
	static final DefaultEmitHelper NO_BUSY_LOOPING = new DefaultEmitHelper(false, false,  0L, 0L);

	final long busyLoopNanos;
	final long maxLoops;
	final boolean busyLoopConcurrentTerminal;
	final boolean busyLoopConcurrentOnNext;

	private DefaultEmitHelper(boolean busyLoopConcurrentOnNext, boolean busyLoopConcurrentTerminal, long busyLoopNanos, long maxLoops) {
		this.busyLoopNanos = busyLoopNanos;
		this.maxLoops = maxLoops;
		this.busyLoopConcurrentOnNext = busyLoopConcurrentOnNext;
		this.busyLoopConcurrentTerminal = busyLoopConcurrentTerminal;
	}

	static void errorFromNext(Sinks.Many<?> sink, Throwable error) {
		if (sink.tryEmitError(error).hasFailed()) {
			//equivalent to multicast as long as context is multiSubscriberContext
			Operators.onErrorDropped(error, sink.currentContext());
		}
	}

	static void errorFromNext(Sinks.One<?> sink, Throwable error) {
		if (sink.tryEmitError(error).hasFailed()) {
			//equivalent to multicast as long as context is multiSubscriberContext
			Operators.onErrorDropped(error, sink.currentContext());
		}
	}

	void completeNoLooping(Sinks.Emission result, Context context) {
		switch (result) {
			case OK:
			case FAIL_CANCELLED:
			case FAIL_TERMINATED:
				break;
			case FAIL_NON_SERIALIZED:
				Operators.onErrorDropped(new IllegalStateException("emitComplete called in parallel with another signal"),
						context);
				break;
			case FAIL_OVERFLOW:
			case FAIL_ZERO_SUBSCRIBER:
				//shouldn't happen
				throw new IllegalStateException(result + " during emitComplete");
		}
	}

	@Override
	public void emitComplete(Sinks.Many<?> sink) {
		Sinks.Emission result = sink.tryEmitComplete();
		if (result == FAIL_NON_SERIALIZED && busyLoopConcurrentTerminal) {
			long loops = 0L;
			do {
				if (loops++ > maxLoops) {
					Operators.onErrorDropped(new IllegalStateException("emitComplete busy looping reached maximum loop count")
							, sink.currentContext());
				}
				LockSupport.parkNanos(busyLoopNanos);
				result = sink.tryEmitComplete();
			} while (result == FAIL_NON_SERIALIZED);
		}

		completeNoLooping(result, sink.currentContext());
	}

	@Override
	public void emitEmpty(Sinks.Empty<?> sink) {
		Sinks.Emission result = sink.tryEmitEmpty();
		if (result == FAIL_NON_SERIALIZED && busyLoopConcurrentTerminal) {
			long loops = 0L;
			do {
				if (loops++ > maxLoops) {
					Operators.onErrorDropped(new IllegalStateException("emitComplete busy looping reached maximum loop count")
							, sink.currentContext());
				}
				LockSupport.parkNanos(busyLoopNanos);
				result = sink.tryEmitEmpty();
			} while (result == FAIL_NON_SERIALIZED);
		}

		completeNoLooping(result, sink.currentContext());
	}

	void errorNoLooping(Sinks.Emission result, Throwable error, Context sinkContext) {
		switch (result) {
			case OK:
			case FAIL_CANCELLED:
				break;
			case FAIL_NON_SERIALIZED:
				Operators.onErrorDropped(new IllegalStateException("emitError called in parallel with another signal", error), sinkContext);
				break;
			case FAIL_TERMINATED:
				Operators.onErrorDropped(error, sinkContext);
				break;
			case FAIL_OVERFLOW:
			case FAIL_ZERO_SUBSCRIBER:
				//shouldn't happen
				throw new IllegalStateException(result + " during emitError");
		}
	}

	@Override
	public void emitError(Sinks.Many<?> sink, Throwable error) {
		Sinks.Emission result = sink.tryEmitError(error);
		if (result == FAIL_NON_SERIALIZED && busyLoopConcurrentTerminal) {
			long loops = 0L;
			do {
				if (loops++ > maxLoops) {
					Operators.onErrorDropped(new IllegalStateException("emitError busy looping reached maximum loop count", error)
							, sink.currentContext());
				}
				LockSupport.parkNanos(busyLoopNanos);
				result = sink.tryEmitError(error);
			} while (result == FAIL_NON_SERIALIZED);
		}

		errorNoLooping(result, error, sink.currentContext());
	}

	@Override
	public void emitError(Sinks.Empty<?> sink, Throwable error) {
		Sinks.Emission result = sink.tryEmitError(error);
		if (result == FAIL_NON_SERIALIZED && busyLoopConcurrentTerminal) {
			long loops = 0L;
			do {
				if (loops++ > maxLoops) {
					Operators.onErrorDropped(new IllegalStateException("emitError busy looping reached maximum loop count", error)
							, sink.currentContext());
				}
				LockSupport.parkNanos(busyLoopNanos);
				result = sink.tryEmitError(error);
			} while (result == FAIL_NON_SERIALIZED);
		}

		errorNoLooping(result, error, sink.currentContext());
	}

	@Override
	public <T> void emitNext(Sinks.Many<T> sink, T v) {
		Sinks.Emission result = sink.tryEmitNext(v);
		if (result == FAIL_NON_SERIALIZED && busyLoopConcurrentOnNext) {
			long loops = 0L;
			do {
				if (loops++ > maxLoops) {
					Operators.onDiscard(v, sink.currentContext());
					errorFromNext(sink, new IllegalStateException("emitNext busy looping reached maximum loop count"));
					return;
				}
				LockSupport.parkNanos(busyLoopNanos);
				result = sink.tryEmitNext(v);
			} while (result == FAIL_NON_SERIALIZED);
		}

		//code can't be mutualized between Many and One due to the need to call emitError
		switch (result) {
			case OK:
				break;
			case FAIL_ZERO_SUBSCRIBER: //most likely no-op until sinks have their own base context?
			case FAIL_CANCELLED:
				Operators.onDiscard(v, sink.currentContext());
				break;
			case FAIL_NON_SERIALIZED:
				Operators.onDiscard(v, sink.currentContext());
				//this effectively terminates the sink, we've failed or opted out of busy looping
				errorFromNext(sink, new IllegalStateException("emitNext called in parallel with another signal"));
				break;
			case FAIL_TERMINATED:
				//equivalent to multicast as long as context is multiSubscriberContext
				Operators.onNextDropped(v, sink.currentContext());
				break;
			case FAIL_OVERFLOW:
				Operators.onDiscard(v, sink.currentContext());
				//this effectively terminates the sink
				errorFromNext(sink, Exceptions.failWithOverflow("Backpressure overflow during emitNext"));
				break;
		}
	}

	@Override
	public <T> void emitValue(Sinks.One<T> sink, @Nullable T v) {
		if (v == null) {
			emitEmpty(sink);
			return;
		}

		Sinks.Emission result = sink.tryEmitValue(v);
		if (result == FAIL_NON_SERIALIZED && busyLoopConcurrentOnNext) {
			long loops = 0L;
			do {
				if (loops++ > maxLoops) {
					Operators.onDiscard(v, sink.currentContext());
					errorFromNext(sink, new IllegalStateException("emitNext busy looping reached maximum loop count"));
					return;
				}
				LockSupport.parkNanos(busyLoopNanos);
				result = sink.tryEmitValue(v);
			} while (result == FAIL_NON_SERIALIZED);
		}

		//code can't be mutualized between Many and One due to the need to call emitError
		switch (result) {
			case OK:
				break;
			case FAIL_ZERO_SUBSCRIBER: //most likely no-op until sinks have their own base context?
			case FAIL_CANCELLED:
				Operators.onDiscard(v, sink.currentContext());
				break;
			case FAIL_NON_SERIALIZED:
				Operators.onDiscard(v, sink.currentContext());
				//this effectively terminates the sink, we've failed or opted out of busy looping
				errorFromNext(sink, new IllegalStateException("emitNext called in parallel with another signal"));
				break;
			case FAIL_TERMINATED:
				//equivalent to multicast as long as context is multiSubscriberContext
				Operators.onNextDropped(v, sink.currentContext());
				break;
			case FAIL_OVERFLOW:
				Operators.onDiscard(v, sink.currentContext());
				//this effectively terminates the sink
				errorFromNext(sink, Exceptions.failWithOverflow("Backpressure overflow during emitNext"));
				break;
		}
	}
}

final class ThrowingEmitHelper implements EmitHelper {

	static final ThrowingEmitHelper INSTANCE = new ThrowingEmitHelper();

	private ThrowingEmitHelper() { }


	@Override
	public void emitComplete(Sinks.Many<?> sink) {
		sink.tryEmitComplete().orThrow();
	}

	@Override
	public void emitEmpty(Sinks.Empty<?> sink) {
		sink.tryEmitEmpty().orThrow();
	}

	@Override
	public void emitError(Sinks.Many<?> sink, Throwable error) {
		sink.tryEmitError(error).orThrowWithCause(error);
	}

	@Override
	public void emitError(Sinks.Empty<?> sink, Throwable error) {
		sink.tryEmitError(error).orThrowWithCause(error);
	}

	@Override
	public <T> void emitNext(Sinks.Many<T> sink, T v) {
		sink.tryEmitNext(v).orThrow();
	}

	@Override
	public <T> void emitValue(Sinks.One<T> sink, T v) {
		sink.tryEmitValue(v).orThrow();
	}
}