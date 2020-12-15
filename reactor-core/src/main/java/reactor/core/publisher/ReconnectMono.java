package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class ReconnectMono<T> extends MonoOperator<T, T> implements Disposable {

	static final CancellationException ON_DISPOSE = new CancellationException("Disposed");

	final ReconnectMainSubscriber<T> mainSubscriber = new ReconnectMainSubscriber<>(this);

	volatile int wip;

	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<ReconnectMono> WIP =
			AtomicIntegerFieldUpdater.newUpdater(ReconnectMono.class, "wip");

	volatile ReconnectInner<T>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<ReconnectMono, ReconnectInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(
					ReconnectMono.class, ReconnectInner[].class, "subscribers");

	@SuppressWarnings("unchecked")
	static final ReconnectInner<?>[] EMPTY_UNSUBSCRIBED = new ReconnectInner[0];

	@SuppressWarnings("unchecked")
	static final ReconnectInner<?>[] EMPTY_SUBSCRIBED = new ReconnectInner[0];

	@SuppressWarnings("unchecked")
	static final ReconnectInner<?>[] READY = new ReconnectInner[0];

	@SuppressWarnings("unchecked")
	static final ReconnectInner<?>[] TERMINATED = new ReconnectInner[0];

	static final int ADDED_STATE = 0;
	static final int READY_STATE = 1;
	static final int TERMINATED_STATE = 2;

	T value;
	Throwable t;

	public ReconnectMono(Mono<? extends T> source, Function<T, >) {
		super(source);
		SUBSCRIBERS.lazySet(this, EMPTY_UNSUBSCRIBED);
	}

	private void doOnValueResolved(T value) {
		// no ops
	}

	protected void doOnValueExpired(T value) {
		// no ops
	}

	protected void doOnDispose() {
		// no ops
	}

	protected void doSubscribe() {
		source.subscribe(mainSubscriber);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		final ReconnectInner<T> reconnectInner = new ReconnectInner<>(actual, this);
		for (; ; ) {
			final int state = this.add(reconnectInner);

			T value = this.value;

			if (state == READY_STATE) {
				if (value != null) {
					reconnectInner.onNext(value);
					reconnectInner.onComplete();
					return;
				}
				// value == null means racing between invalidate and this subscriber
				// thus, we have to loop again
				continue;
			} else if (state == TERMINATED_STATE) {
				reconnectInner.onError(this.t);
				return;
			}

			return;
		}
	}

	public void connect() {

	}

	@Override
	public final void dispose() {
		this.terminate(ON_DISPOSE);
	}

	@Override
	public final boolean isDisposed() {
		return this.subscribers == TERMINATED;
	}

	public final boolean isPending() {
		ReconnectInner<T>[] state = this.subscribers;
		return state != READY && state != TERMINATED;
	}

//	@Nullable
//	public final T valueIfResolved() {
//		if (this.subscribers == READY) {
//			T value = this.value;
//			if (value != null) {
//				return value;
//			}
//		}
//
//		return null;
//	} TODO: remove

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code
	 * ReconnectMono}. If the {@link ResolvingOperator} is completed with an error a RuntimeException
	 * that wraps the error is thrown.
	 *
	 * @param timeout the timeout value as a {@link Duration}
	 * @return the value of this {@link ResolvingOperator} or {@code null} if the timeout is reached
	 *     and the {@link ResolvingOperator} has not completed
	 * @throws RuntimeException if terminated with error
	 * @throws IllegalStateException if timed out or {@link Thread} was interrupted with {@link
	 *     InterruptedException}
	 */
	@Nullable
	@SuppressWarnings({"uncheked", "BusyWait"})
	public T block(@Nullable Duration timeout) {
		try {
			ReconnectInner<T>[] subscribers = this.subscribers;
			if (subscribers == READY) {
				final T value = this.value;
				if (value != null) {
					return value;
				} else {
					// value == null means racing between invalidate and this block
					// thus, we have to update the state again and see what happened
					subscribers = this.subscribers;
				}
			}

			if (subscribers == TERMINATED) {
				RuntimeException re = Exceptions.propagate(this.t);
				re = Exceptions.addSuppressed(re, new Exception("Terminated with an error"));
				throw re;
			}

			// connect once
			if (subscribers == EMPTY_UNSUBSCRIBED
					&& SUBSCRIBERS.compareAndSet(this, EMPTY_UNSUBSCRIBED, EMPTY_SUBSCRIBED)) {
				this.doSubscribe();
			}

			long delay;
			if (null == timeout) {
				delay = 0L;
			} else {
				delay = System.nanoTime() + timeout.toNanos();
			}
			for (; ; ) {
				ReconnectInner<T>[] inners = this.subscribers;

				if (inners == READY) {
					final T value = this.value;
					if (value != null) {
						return value;
					} else {
						// value == null means racing between invalidate and this block
						// thus, we have to update the state again and see what happened
						inners = this.subscribers;
					}
				}
				if (inners == TERMINATED) {
					RuntimeException re = Exceptions.propagate(this.t);
					re = Exceptions.addSuppressed(re, new Exception("Terminated with an error"));
					throw re;
				}
				if (timeout != null && delay < System.nanoTime()) {
					throw new IllegalStateException("Timeout on Mono blocking read");
				}

				Thread.sleep(1);
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();

			throw new IllegalStateException("Thread Interruption on Mono blocking read");
		}
	}

	@SuppressWarnings("unchecked")
	final void terminate(Throwable t) {
		if (isDisposed()) {
			return;
		}

		// writes happens before volatile write
		this.t = t;

		final ReconnectInner<T>[] subscribers = SUBSCRIBERS.getAndSet(this, TERMINATED);
		if (subscribers == TERMINATED) {
			Operators.onErrorDropped(t, Context.empty());
			return;
		}

		this.doOnDispose();

		this.doFinally();

		for (ReconnectInner<T> consumer : subscribers) {
			consumer.onError(t);
		}
	}

	final void complete(T value) {
		ReconnectInner<T>[] subscribers = this.subscribers;
		if (subscribers == TERMINATED) {
			this.doOnValueExpired(value);
			return;
		}

		this.value = value;

		for (; ; ) {
			// ensures TERMINATE is going to be replaced with READY
			if (SUBSCRIBERS.compareAndSet(this, subscribers, READY)) {
				break;
			}

			subscribers = this.subscribers;

			if (subscribers == TERMINATED) {
				this.doFinally();
				return;
			}
		}

		this.doOnValueResolved(value);

		for (ReconnectInner<T> consumer : subscribers) {
			consumer.onNext(value);
			consumer.onComplete();
		}
	}

	final void doFinally() {
		if (WIP.getAndIncrement(this) != 0) {
			return;
		}

		int m = 1;
		T value;

		for (; ; ) {
			value = this.value;
			if (value != null && isDisposed()) {
				this.value = null;
				this.doOnValueExpired(value);
				return;
			}

			m = WIP.addAndGet(this, -m);
			if (m == 0) {
				return;
			}
		}
	}

	final void reset() {
		if (this.subscribers == TERMINATED) {
			return;
		}

		final ReconnectInner<T>[] subscribers = this.subscribers;

		if (subscribers == READY) {
			// guarded section to ensure we expire value exactly once if there is racing
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			final T value = this.value;
			if (value != null) {
				this.value = null;
				this.doOnValueExpired(value);
			}

			int m = 1;
			for (; ; ) {
				if (isDisposed()) {
					return;
				}

				m = WIP.addAndGet(this, -m);
				if (m == 0) {
					break;
				}
			}

			SUBSCRIBERS.compareAndSet(this, READY, EMPTY_UNSUBSCRIBED);
		}
	}

	final int add(ReconnectInner<T> ps) {
		for (; ; ) {
			ReconnectInner<T>[] a = this.subscribers;

			if (a == TERMINATED) {
				return TERMINATED_STATE;
			}

			if (a == READY) {
				return READY_STATE;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			ReconnectInner<T>[] b = new ReconnectInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				if (a == EMPTY_UNSUBSCRIBED) {
					this.doSubscribe();
				}
				return ADDED_STATE;
			}
		}
	}

	@SuppressWarnings("unchecked")
	final void remove(ReconnectInner<T> ps) {
		for (; ; ) {
			ReconnectInner<T>[] a = this.subscribers;
			int n = a.length;
			if (n == 0) {
				return;
			}

			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			ReconnectInner<?>[] b;

			if (n == 1) {
				b = EMPTY_SUBSCRIBED;
			} else {
				b = new ReconnectInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	final static class ReconnectInner<T> extends Operators.MonoSubscriber<T, T> {
		final ReconnectMono<T> parent;

		ReconnectInner(CoreSubscriber<? super T> actual, ReconnectMono<T> parent) {
			super(actual);
			this.parent = parent;
		}

		@Override
		public void cancel() {
			if (STATE.getAndSet(this, CANCELLED) != CANCELLED) {
				parent.remove(this);
			}
		}

		@Override
		public void onComplete() {
			if (!isCancelled()) {
				actual.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (!isCancelled()) {
				actual.onError(t);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return parent;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			return super.scanUnsafe(key);
		}
	}

	static final class ReconnectMainSubscriber<T> implements CoreSubscriber<T> {

		final ReconnectMono<T> parent;

		volatile Subscription s;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ReconnectMainSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(
						ReconnectMainSubscriber.class, Subscription.class, "s");

		volatile int wip;

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ReconnectMainSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ReconnectMainSubscriber.class, "wip");

		T value;

		ReconnectMainSubscriber(ReconnectMono<T> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onComplete() {
			final Subscription s = this.s;
			final T value = this.value;

			if (s == Operators.cancelledSubscription() || !S.compareAndSet(this, s, null)) {
				this.doFinally();
				return;
			}

			final ReconnectMono<T> p = this.parent;
			if (value == null) {
				p.terminate(new IllegalStateException("Source completed empty"));
			} else {
				p.complete(value);
			}
		}

		@Override
		public void onError(Throwable t) {
			final Subscription s = this.s;

			if (s == Operators.cancelledSubscription()
					|| S.getAndSet(this, Operators.cancelledSubscription())
					== Operators.cancelledSubscription()) {
				this.doFinally();
				Operators.onErrorDropped(t, Context.empty());
				return;
			}

			this.doFinally();
			// terminate upstream which means retryBackoff has exhausted
			this.parent.terminate(t);
		}

		@Override
		public void onNext(T value) {
			if (this.s == Operators.cancelledSubscription()) {
				this.parent.doOnValueExpired(value);
				return;
			}

			this.value = value;
			// volatile write and check on racing
			this.doFinally();
		}

		void dispose() {
			if (Operators.terminate(S, this)) {
				this.doFinally();
			}
		}

		final void doFinally() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int m = 1;
			T value;

			for (; ; ) {
				value = this.value;
				if (value != null && this.s == Operators.cancelledSubscription()) {
					this.value = null;
					this.parent.doOnValueExpired(value);
					return;
				}

				m = WIP.addAndGet(this, -m);
				if (m == 0) {
					return;
				}
			}
		}
	}

	interface Invalidatable {

		void reset();
	}
}
