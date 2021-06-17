/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.total;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Tests the utility methods in {@link org.hibernate.reactive.util.impl.CompletionStages}
 */
@RunWith(VertxUnitRunner.class)
public class CompletionStagesTest {

	private final Object[] entries = { "a", "b", "c", "d", "e" };
	private final List<Object> looped = new ArrayList<>();

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
	public void testLoopOnArrayIndex(TestContext context) {
		test( context, loop( 0, entries.length, index -> completedFuture( looped.add( entries[index] ) ) )
				.thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnArray(TestContext context) {
		test( context, loop( entries, entry -> completedFuture( looped.add( entry ) ) )
				.thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnArrayWithFilter(TestContext context) {
		test( context, loop(
				entries,
				index -> entries[index].equals( "a" ),
				index -> {
					// Test that the filter is not executed ahead of time
					if ( index == 0 ) {
						entries[1] = "a";
					}
					return completedFuture( looped.add( entries[index] ) );
				}
		).thenAccept( v -> assertThat( looped ).containsExactly( "a", "a" ) ) );
	}

	@Test
	public void testLoopOnIteratorWithIndex(TestContext context) {
		test( context, loop( iterator( entries ), (entry, index) -> {
			assertThat( entry ).isEqualTo( entries[index] );
			return completedFuture( looped.add( entry ) );
		} ).thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnIterable(TestContext context) {
		test( context, loop( asList( entries ), entry -> completedFuture( looped.add( entry ) ) )
				.thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnIteratorWithFilter(TestContext context) {
		test( context, loop(
				iterator( entries ),
				(entry, index) -> entry.equals( "a" ),
				(entry, index) -> {
					// Test that the filter is not executed ahead of time
					if ( index == 0 ) {
						entries[1] = "a";
					}
					return completedFuture( looped.add( entry ) );
				}
		).thenAccept( v -> assertThat( looped ).containsExactly( "a", "a" ) ) );
	}

	@Test
	public void testLoopOnArrayIndexWithFilter(TestContext context) {
		test( context, loop(
				entries,
				index -> entries[index].equals( "a" ),
				index -> {
					// Test that the filter is not executed ahead of time
					if ( index == 0 ) {
						entries[1] = "a";
					}
					return completedFuture( looped.add( entries[index] ) );
				}
		).thenAccept( v -> assertThat( looped ).containsExactly( "a", "a" ) ) );
	}

	@Test
	public void testLoopOnQueueWithFilter(TestContext context) {
		final Queue<Object> queue = new LinkedList<>( asList( entries ) );
		test( context, loop(
				queue,
				entry -> completedFuture( looped.add( entry ) )
		).thenAccept( v -> assertThat( looped ).containsExactly( entries ) ) );
	}

	@Test
	public void testLoopOnSetWithFilter(TestContext context) {
		test( context, loop(
				new LinkedHashSet<>( asList( entries ) ),
				entry -> entry.equals( "c" ),
				entry -> completedFuture( looped.add( entry ) )
		).thenAccept( v -> assertThat( looped ).containsExactly( "c" ) ) );
	}

	@Test
	public void testLoopOnCollectionWithFilter(TestContext context) {
		final Collection<Object> list = asList( entries );
		test( context, loop(
				list,
				entry -> entry.equals( "c" ),
				entry -> completedFuture( looped.add( entry ) )
		).thenAccept( v -> assertThat( looped ).containsExactly( "c" ) ) );
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
