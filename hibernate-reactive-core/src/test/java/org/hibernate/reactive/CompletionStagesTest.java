/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;


import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.hibernate.reactive.util.impl.CompletionStages.*;
import static org.hibernate.reactive.util.impl.CompletionStages.total;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.loopWithoutTrampoline;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the utility methods in {@link org.hibernate.reactive.util.impl.CompletionStages}
 */
@RunWith(VertxUnitRunner.class)
public class CompletionStagesTest {

	private Object[] entries = { "a", "b", "c", "d", "e" };
	private List<Object> looped = new ArrayList<>();

	@Test
	public void testTotalWithIntegers(TestContext context) {
		int startInt = 0;
		int endInt = entries.length;
		int expectedTotal = IntStream.range( startInt, endInt ).sum();

		test( context, total( startInt, endInt, index -> completedFuture( looped.add( entries[index] ) )
				.thenApply( unused -> index ) )
				.thenAccept( total -> {
					assertThat( total ).isEqualTo( expectedTotal );
					assertThat( looped ).containsExactly( entries );
				} ) );
	}

	@Test
	public void testTotalWithIterator(TestContext context) {
		int increment = 3;

		test( context, total( iterator( entries ), entry -> voidFuture()
				.thenAccept( v -> looped.add( entry ) )
				.thenApply( v -> increment ) )
				.thenAccept( total -> {
					assertThat( total ).isEqualTo( entries.length * increment );
					assertThat( looped ).containsExactly( entries );
				} )
		);
	}

	@Test
	public void testTotalWithArray(TestContext context) {
		int increment = 2;

		test( context, total( entries, entry -> voidFuture()
				.thenAccept( v -> looped.add( entry ) )
				.thenApply( v -> increment ) )
				.thenAccept( total -> {
					assertThat( total ).isEqualTo( entries.length * increment );
					assertThat( looped ).containsExactly( entries );
				} )
		);
	}

	@Test
	public void testLoopOnArray(TestContext context) {
		test( context, loop( entries, entry -> completedFuture( looped.add( entry ) ) )
				.thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnIterator(TestContext context) {
		test( context, loop( iterator( entries ), entry -> completedFuture( looped.add( entry ) ) )
				.thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnIterable(TestContext context) {
		test( context, loop( asList( entries ), entry -> completedFuture( looped.add( entry ) ) )
				.thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnStream(TestContext context) {
		test( context, loop( stream( entries ), entry -> completedFuture( looped.add( entry ) ) )
				.thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnIntStream(TestContext context) {
		test( context, loop(
				IntStream.range( 0, entries.length ),
				index -> completedFuture( looped.add( entries[index] ) )
			  ).thenAccept( v -> assertThat( looped ).containsExactly( entries ) )
		);
	}

	@Test
	public void testLoopOnIntStreamWithoutTrampoline(TestContext context) {
		test( context, loopWithoutTrampoline(
				IntStream.range( 0, entries.length ),
				index -> completedFuture( looped.add( entries[index] ) )
			  ).thenAccept( v -> assertThat( looped ).containsExactly( entries ) )
		);
	}

	@Test
	public void testLoopWithIteratorWithoutTrampoline(TestContext context) {
		test( context, loopWithoutTrampoline(
				iterator( entries ),
				entry -> completedFuture( looped.add( entry ) )
			  ).thenAccept( v -> assertThat( looped ).containsExactly( entries ) )
		);
	}

	private static Iterator<Object> iterator(Object[] entries) {
		return asList( entries ).iterator();
	}

	private static void test(TestContext context, CompletionStage<?> cs) {
		Async async = context.async();
		cs.whenComplete( (res, err) -> {
			if ( err != null ) {
				context.fail( err );
			}
			else {
				async.complete();
			}
		} );
	}
}
