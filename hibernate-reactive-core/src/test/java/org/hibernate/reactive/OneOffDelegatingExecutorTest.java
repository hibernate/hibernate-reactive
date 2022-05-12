/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.hibernate.reactive.context.impl.OneOffDelegatingExecutor;

import org.junit.ComparisonFailure;
import org.junit.Test;

import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

public class OneOffDelegatingExecutorTest {

	@Test
	public void test() {
		final Executor executor = new ThreadPerTaskExecutor();

		OneOffDelegatingExecutor taskControl = new OneOffDelegatingExecutor( executor );

		CompletableFuture<Integer> supplier = CompletableFuture.supplyAsync(
				OneOffDelegatingExecutorTest::asyncIntegerGeneration,
				taskControl
		);
		sleep2seconds();
		CompletableFuture<Void> future = supplier
				.thenApply( OneOffDelegatingExecutorTest::incrementInput )
				.thenAccept( OneOffDelegatingExecutorTest::checkResult );

		taskControl.runHeldTasks();

		future.join();
	}

	@Test
	public void testWithoutOneOffExecutor() {
		final Executor executor = new ThreadPerTaskExecutor();

		CompletionStage<Integer> supplier = CompletableFuture.supplyAsync(
				OneOffDelegatingExecutorTest::asyncIntegerGeneration,
				executor
		);
		sleep2seconds();
		CompletionStage<Void> future = supplier
				.thenApply( OneOffDelegatingExecutorTest::incrementInput )
				.thenAccept( OneOffDelegatingExecutorTest::checkResult );

		assertThrown( ComparisonFailure.class, future )
				.toCompletableFuture()
				.join();
	}

	private static class ThreadPerTaskExecutor implements Executor {

		public static final String THREAD_NAME = "Thread4Testing";

		@Override
		public void execute(@NotNull Runnable command) {
			new Thread( command, THREAD_NAME ).start();
		}
	}

	private static Integer asyncIntegerGeneration() {
		assertThat( Thread.currentThread().getName() ).isEqualTo( ThreadPerTaskExecutor.THREAD_NAME );
		return 1;
	}

	private static void sleep2seconds() {
		try {
			Thread.sleep( 1000 );
		}
		catch (InterruptedException ignored) {
			ignored.printStackTrace();
		}
	}

	private static Integer incrementInput(Integer i) {
		assertThat( Thread.currentThread().getName() ).isEqualTo( ThreadPerTaskExecutor.THREAD_NAME );
		return i + 1;
	}

	private static void checkResult(Integer i) {
		assertThat( Thread.currentThread().getName() ).isEqualTo( ThreadPerTaskExecutor.THREAD_NAME );
		assertThat( i ).isEqualTo( 2 );
	}
}
