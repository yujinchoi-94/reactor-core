/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.core.Fuseable;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.function.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSubscriberTest {

	public static final int ITERATION_COUNT = 50000;

	@Test
	public void queueSubscriptionSyncRejected() {
		MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(new AssertSubscriber<>());

		assertThat(ds.requestFusion(Fuseable.SYNC)).isEqualTo(Fuseable.NONE);
	}

	@Test
	public void clear() {
		MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(new AssertSubscriber<>());

		ds.value = 1;

		ds.clear();

		assertThat(ds.state).isEqualTo(MonoSubscriber.FUSED_CONSUMED);
		assertThat(ds.value).isNull();
	}

	@Test
	public void completeCancelRace() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>(Operators.enableOnDiscard(null, v -> discarded.incrementAndGet()), Long.MAX_VALUE);
			final MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(ts);
			ts.onSubscribe(ds);

			Runnable r1 = () -> ds.complete(1);
			Runnable r2 = ds::cancel;

			RaceTestUtils.race(r1, r2);

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			} else {
				assertThat(discarded.get()).isGreaterThanOrEqualTo(1); // TODO fix to exactly once
			}

		}
	}

	@Test
	public void requestCancelRace() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>(Operators.enableOnDiscard(null, v -> discarded.incrementAndGet()), 0L);

			final MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(ts);
			ts.onSubscribe(ds);
			ds.complete(1);

			Runnable r1 = () -> ds.request(1);
			Runnable r2 = ds::cancel;

			RaceTestUtils.race(r1, r2);

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			} else {
				assertThat(discarded.get()).isGreaterThanOrEqualTo(1); // TODO fix to exactly once
			}
		}
	}

	@Test
	public void requestCancelOnNextRace() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			AtomicInteger discarded = new AtomicInteger();
			AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>(Operators.enableOnDiscard(null, v -> discarded.incrementAndGet()), 0L);

			final MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(ts);
			ts.onSubscribe(ds);

			Runnable r1 = () -> ds.request(1);
			Runnable r2 = ds::cancel;
			Runnable r3 = () -> ds.complete(1);

			RaceTestUtils.race(r1, r2, r3);

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			} else {
				assertThat(discarded.get()).isGreaterThanOrEqualTo(1); // TODO fix to exactly once
			}
		}
	}

	@Test
	public void issue1719() {
		for (int i = 0; i < ITERATION_COUNT; i++) {
			Map<String, Mono<Integer>> input = new HashMap<>();
			input.put("one", Mono.just(1));
			input.put("two", Mono.create(
					(sink) -> Schedulers.elastic().schedule(() -> sink.success(2))));
			input.put("three", Mono.just(3));
			int sum = Flux.fromIterable(input.entrySet())
			              .flatMap((entry) -> Mono.zip(Mono.just(entry.getKey()), entry.getValue()))
			              .collectMap(Tuple2::getT1, Tuple2::getT2).map((items) -> {
						AtomicInteger result = new AtomicInteger();
						items.values().forEach(result::addAndGet);
						return result.get();
					}).block();
			assertThat(sum).as("Iteration %s", i).isEqualTo(6);
		}
	}
}
