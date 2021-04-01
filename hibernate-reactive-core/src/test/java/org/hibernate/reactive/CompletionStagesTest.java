/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;

import org.hibernate.reactive.util.impl.CompletionStages;

import static org.hibernate.reactive.util.impl.CompletionStages.total;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;
import static org.hibernate.reactive.util.impl.CompletionStages.loopWithoutTrampoline;

import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.assertj.core.api.Assertions;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(VertxUnitRunner.class)
public class CompletionStagesTest {

	protected static void test(TestContext context, CompletionStage<?> cs) {
		// this will be added to TestContext in the next vert.x release
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

	@Test
	public void testTotalWithIntegers(TestContext context) {
		int startInt = 0;
		int endInt = 11;

		// create a value of 'index + 2' and add it to the total. Result should be 77
		test(   context,
			    total( startInt, endInt, index -> CompletionStages.completedFuture( index + 2 ) )
					  .thenAccept( total -> Assertions.assertThat( total ).isEqualTo( 77 ) )
		);
	}

	@Test
	public void testTotalWithIterator(TestContext context) {
		List<Object> entries = Arrays.asList( "a", "b", "c", "d", "e" );
		int incrementAddedPerEntry = 3;
		List<Object> looped = new ArrayList<>();

		test(   context,
				total(
						entries.iterator(),
						entry -> CompletionStages.voidFuture()
								.thenAccept( v -> looped.add( entry ) )
								.thenApply( v -> incrementAddedPerEntry )
				)
						.thenAccept( total -> {
							assertThat( total ).isEqualTo( entries.size() * incrementAddedPerEntry );
							assertThat( looped ).containsExactly( entries.toArray( new Object[entries.size()] ) );
						} )
		);
	}

	@Test
	public void testTotalWithArray(TestContext context) {
		String[] entries = { "a", "b", "c", "d", "e" };
		int incrementAddedPerEntry = 2;
		List<Object> looped = new ArrayList<>();

		test(   context,
				total( entries, entry -> CompletionStages.voidFuture()
						.thenAccept( v -> looped.add( entry ) )
						.thenApply( v -> incrementAddedPerEntry ) )
						.thenAccept( total -> {
							assertThat( total ).isEqualTo( entries.length * incrementAddedPerEntry );
							assertThat( looped ).containsExactly( entries );
						} )
		);
	}

	@Test
	public void testLoopOnArray(TestContext context) {
		String[] entries = { "a", "b", "c", "d", "e" };
		List<String> looped = new ArrayList<>();

		test(   context,
				loop( entries, entry -> CompletionStages.completedFuture( looped.add( entry ) ) )
						.thenAccept( count -> assertThat( looped )
								.containsExactly( entries ) )
		);
	}

	@Test
	public void testLoopOnIterator(TestContext context) {
		List<Object> entries = Arrays.asList( "a", "b", "c", "d", "e" );
		List<Object> looped = new ArrayList<>();

		test(   context,
				loop(
						entries.iterator(),
						entry -> CompletionStages.completedFuture( looped.add( entry ) )
				)
						.thenAccept( count -> assertThat( looped )
								.containsExactly( entries.toArray( new Object[entries.size()] ) ) )
		);
	}

	@Test
	public void testLoopOnIterable(TestContext context) {
		List<String> entries = Arrays.asList( "a", "b", "c", "d", "e" );
		List<String> looped = new ArrayList<>();

		test(   context,
				loop(
						entries,
						entry -> CompletionStages.completedFuture( looped.add( entry ) )
				).thenAccept( count -> assertThat( looped )
						.containsExactly( entries.toArray( new String[entries.size()] ) ) )
		);
	}

	@Test
	public void testLoopOnStream(TestContext context) {
		String[] entries = { "a", "b", "c", "d", "e" };
		List<String> looped = new ArrayList<>();

		test(   context,
				loop(
						Arrays.stream( entries ),
						entry -> CompletionStages.completedFuture( looped.add( entry ) )
				)
						.thenAccept( count -> assertThat( looped.toArray( new String[looped.size()] ) )
								.containsExactly( entries ) )
		);
	}

	@Test
	public void testLoopOnIntStream(TestContext context) {
		List<Object> entries = Arrays.asList( "a", "b", "c", "d", "e" );
		List<Object> looped = new ArrayList<>();

		test(   context,
				loop(
						IntStream.range( 0, entries.size() ),
						index -> CompletionStages.voidFuture( looped.add( entries.get( index ) ) )
				).thenAccept( count -> assertThat( looped )
						.containsExactly( entries.toArray( new Object[entries.size()] ) ) )
		);
	}

	@Test
	public void testLoopOnIntStreamWithoutTrampoline(TestContext context) {
		List<Object> entries = Arrays.asList( "a", "b", "c", "d", "e" );
		List<Object> looped = new ArrayList<>();

		test(   context,
				loopWithoutTrampoline(
						IntStream.range( 0, entries.size() ),
						index -> CompletionStages.voidFuture( looped.add( entries.get( index ) ) )
				).thenAccept( count -> assertThat( looped )
						.containsExactly( entries.toArray( new Object[entries.size()] ) ) )
		);
	}

	@Test
	public void testLoopWithIteratorWithoutTrampoline(TestContext context) {
		List<Object> entries = Arrays.asList( "a", "b", "c", "d", "e" );
		List<Object> looped = new ArrayList<>();

		test(   context,
				loopWithoutTrampoline(
						entries.iterator(),
						entry -> CompletionStages.voidFuture( looped.add( entry ) )
				).thenAccept( count -> assertThat( looped )
						.containsExactly( entries.toArray( new Object[entries.size()] ) ) )
		);
	}
}
