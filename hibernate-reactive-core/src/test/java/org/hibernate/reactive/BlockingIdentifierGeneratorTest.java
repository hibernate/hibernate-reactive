/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import org.hibernate.reactive.id.impl.BlockingIdentifierGenerator;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.internal.VertxInternal;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests how {@link BlockingIdentifierGenerator} behaves when many callers
 * need a new block at the same time. It doesn't need a database: the
 * requests for a new "hi" value are completed by the tests.
 */
public class BlockingIdentifierGeneratorTest {

	private static final int BLOCK_SIZE = 50;
	private static final int TIMEOUT_SECONDS = 30;

	private static final ReactiveConnectionSupplier CONNECTION_SUPPLIER = () -> null;
	private static final List<Long> IDS_1_TO_120 = LongStream.rangeClosed( 1, 120 ).boxed().toList();
	private static final List<Long> IDS_1_TO_50 = LongStream.rangeClosed( 1, 50 ).boxed().toList();
	private static final List<Long> IDS_1_TO_10 = LongStream.rangeClosed( 1, 10 ).boxed().toList();
	private static final List<Long> IDS_1_TO_9 = LongStream.rangeClosed( 1, 9 ).boxed().toList();
	private static final List<Long> IDS_11_TO_50 = LongStream.rangeClosed( 11, 50 ).boxed().toList();
	private static final List<Long> IDS_111_TO_150 = LongStream.rangeClosed( 111, 150 ).boxed().toList();
	private static final RuntimeException FAILURE = new RuntimeException( "Expected failure" );

	private static Vertx vertx;

	@BeforeAll
	public static void startVertx() {
		vertx = Vertx.vertx();
	}

	@AfterAll
	public static void stopVertx() throws Exception {
		vertx.close().toCompletionStage().toCompletableFuture().get( TIMEOUT_SECONDS, SECONDS );
	}

	/**
	 * A generator where the test decides when (and how) each request for a new
	 * block completes, or that completes them right away on another thread.
	 */
	private static class FakeGenerator extends BlockingIdentifierGenerator {
		private final int blockSize;
		private final boolean autoComplete;
		private final List<CompletableFuture<Long>> fetches = new CopyOnWriteArrayList<>();
		// The connection used by each request, including the ones that threw an exception
		private final List<ReactiveConnectionSupplier> connections = new CopyOnWriteArrayList<>();
		private final AtomicInteger inFlight = new AtomicInteger();
		private final AtomicInteger maxInFlight = new AtomicInteger();
		private final AtomicLong nextHi = new AtomicLong( 1 );
		private volatile boolean failSynchronously;
		private volatile boolean shared = true;

		FakeGenerator(int blockSize, boolean autoComplete) {
			this.blockSize = blockSize;
			this.autoComplete = autoComplete;
		}

		@Override
		protected int getBlockSize() {
			return blockSize;
		}

		@Override
		protected boolean isNextHiValueShared() {
			return shared;
		}

		@Override
		protected CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session) {
			connections.add( session );
			if ( failSynchronously ) {
				throw FAILURE;
			}
			final CompletableFuture<Long> fetch = new CompletableFuture<>();
			maxInFlight.accumulateAndGet( inFlight.incrementAndGet(), Math::max );
			fetches.add( fetch );
			if ( autoComplete ) {
				CompletableFuture.runAsync( () -> complete( fetch, nextHi.getAndAdd( blockSize ) ) );
			}
			return fetch;
		}

		void complete(CompletableFuture<Long> fetch, long hi) {
			inFlight.decrementAndGet();
			fetch.complete( hi );
		}

		void fail(CompletableFuture<Long> fetch, Throwable throwable) {
			inFlight.decrementAndGet();
			fetch.completeExceptionally( throwable );
		}

		/** Waits until the generator has asked the "database" for {@code count} blocks */
		void awaitFetches(int count) throws InterruptedException {
			final long deadline = System.nanoTime() + SECONDS.toNanos( TIMEOUT_SECONDS );
			while ( fetches.size() < count && System.nanoTime() < deadline ) {
				Thread.sleep( 1 );
			}
			assertEquals( count, fetches.size() );
		}
	}

	/**
	 * A request for an id, together with the context in which it was completed.
	 * Each request is a different connection, like the sessions of different callers.
	 */
	private static class Call implements ReactiveConnectionSupplier {
		private final CompletableFuture<Long> id = new CompletableFuture<>();
		private volatile Context completionContext;

		Long get() throws Exception {
			return id.get( TIMEOUT_SECONDS, SECONDS );
		}

		@Override
		public ReactiveConnection getReactiveConnection() {
			return null;
		}
	}

	/** Asks for an id from the given context */
	private static Call call(FakeGenerator generator, Context context) {
		final Call call = new Call();
		context.runOnContext( v -> generator
				.generate( call, null )
				.whenComplete( (id, throwable) -> {
					call.completionContext = Vertx.currentContext();
					if ( throwable != null ) {
						call.id.completeExceptionally( throwable );
					}
					else {
						call.id.complete( id );
					}
				} ) );
		return call;
	}

	/** Asks for {@code count} ids from the given context and waits until all the requests have been submitted */
	private static List<Call> calls(FakeGenerator generator, Context context, int count) throws Exception {
		final List<Call> calls = new ArrayList<>();
		for ( int i = 0; i < count; i++ ) {
			calls.add( call( generator, context ) );
		}
		// Tasks on a context run in order, so when this one runs all the requests have been submitted
		final CompletableFuture<Void> submitted = new CompletableFuture<>();
		context.runOnContext( v -> submitted.complete( null ) );
		submitted.get( TIMEOUT_SECONDS, SECONDS );
		return calls;
	}

	private static List<Long> ids(List<Call> calls) throws Exception {
		final List<Long> ids = new ArrayList<>();
		for ( Call call : calls ) {
			ids.add( call.get() );
		}
		return ids;
	}

	private static Context newContext() {
		return ( (VertxInternal) vertx ).createEventLoopContext();
	}

	@Test
	public void burstStartsOneFetchEveryBlockSizeCallers() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final List<Call> calls = calls( generator, newContext(), 120 );

		// 120 callers and a block of 50: 3 requests, all in flight at the same time
		generator.awaitFetches( 3 );
		assertEquals( 3, generator.maxInFlight.get() );

		// The order in which the database replies doesn't matter
		generator.complete( generator.fetches.get( 2 ), 101 );
		generator.complete( generator.fetches.get( 1 ), 51 );
		generator.complete( generator.fetches.get( 0 ), 1 );

		final List<Long> ids = ids( calls );
		assertEquals( IDS_1_TO_120, ids.stream().sorted().toList() );
		assertEquals( 3, generator.fetches.size() );
	}

	// Unlike burstStartsOneFetchEveryBlockSizeCallers, the callers don't join the fetch of another caller
	@Test
	public void eachCallerStartsItsOwnFetchWhenFetchesAreNotShared() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		generator.shared = false;
		final Context context = newContext();
		final List<Call> calls = calls( generator, context, 3 );

		generator.awaitFetches( 3 );
		assertEquals( calls, generator.connections );

		generator.complete( generator.fetches.get( 0 ), 1 );
		generator.complete( generator.fetches.get( 1 ), 51 );
		generator.complete( generator.fetches.get( 2 ), 101 );
		assertEquals( List.of( 1L, 51L, 101L ), ids( calls ) );

		// The ids left in one of the blocks are used before starting a new fetch
		assertEquals( 2L, call( generator, context ).get() );
		assertEquals( 3, generator.fetches.size() );
	}

	@Test
	public void eachCallerWaitsForTheFetchItJoined() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final Context context = newContext();
		final List<Call> calls = calls( generator, context, 120 );
		generator.awaitFetches( 3 );

		// Callers are served in order, 50 per fetch: the first fetch serves the first 50 callers only
		generator.complete( generator.fetches.get( 0 ), 1 );
		assertEquals( IDS_1_TO_50, ids( calls.subList( 0, 50 ) ) );
		assertFalse( calls.get( 50 ).id.isDone() );
		assertFalse( calls.get( 119 ).id.isDone() );

		generator.complete( generator.fetches.get( 1 ), 51 );
		ids( calls.subList( 50, 100 ) );
		assertFalse( calls.get( 100 ).id.isDone() );

		generator.complete( generator.fetches.get( 2 ), 101 );
		assertEquals( 101L, calls.get( 100 ).get() );
		assertEquals( 120L, calls.get( 119 ).get() );
		assertEquals( 3, generator.fetches.size() );
	}

	@Test
	public void unusedIdsOfABlockAreReused() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final Context context = newContext();
		final List<Call> firstCalls = calls( generator, context, 10 );
		generator.awaitFetches( 1 );
		generator.complete( generator.fetches.get( 0 ), 1 );
		assertEquals( IDS_1_TO_10, ids( firstCalls ) );

		// The 40 ids left in the block are enough: no new request
		final List<Call> otherCalls = calls( generator, context, 40 );
		assertEquals( IDS_11_TO_50, ids( otherCalls ) );
		assertEquals( 1, generator.fetches.size() );

		// The block is now exhausted
		final Call next = call( generator, context );
		generator.awaitFetches( 2 );
		generator.complete( generator.fetches.get( 1 ), 51 );
		assertEquals( 51L, next.get() );
	}

	@Test
	public void unusedIdsOfABlockAreKeptWhenAFullBlockArrivesLater() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final Context context = newContext();
		// The first fetch is full, the second one has 10 waiters
		final List<Call> calls = calls( generator, context, 60 );
		generator.awaitFetches( 2 );

		generator.complete( generator.fetches.get( 1 ), 101 );
		ids( calls.subList( 50, 60 ) );
		generator.complete( generator.fetches.get( 0 ), 1 );
		ids( calls.subList( 0, 50 ) );

		// 111 to 150 are the left over of the second block
		assertEquals( IDS_111_TO_150, ids( calls( generator, context, 40 ) ) );
		assertEquals( 2, generator.fetches.size() );
	}

	@Test
	public void failedFetchOnlyFailsTheCallerThatStartedIt() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final Context context = newContext();
		final List<Call> calls = calls( generator, context, 10 );
		generator.awaitFetches( 1 );
		assertSame( calls.get( 0 ), generator.connections.get( 0 ) );

		generator.fail( generator.fetches.get( 0 ), FAILURE );
		assertSame( FAILURE, assertThrows( ExecutionException.class, calls.get( 0 )::get ).getCause() );

		// The other callers start a new fetch with the connection of the next caller
		generator.awaitFetches( 2 );
		assertSame( calls.get( 1 ), generator.connections.get( 1 ) );
		assertFalse( calls.get( 1 ).id.isDone() );
		generator.complete( generator.fetches.get( 1 ), 1 );
		assertEquals( IDS_1_TO_9, ids( calls.subList( 1, 10 ) ) );

		// Still working: the left over ids of the block are used
		assertEquals( 10L, call( generator, context ).get() );
		assertEquals( 2, generator.fetches.size() );
	}

	@Test
	public void failedFetchIsNotJoinedByNewCallers() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final Context context = newContext();
		final Call failed = call( generator, context );
		generator.awaitFetches( 1 );
		generator.fail( generator.fetches.get( 0 ), FAILURE );
		assertSame( FAILURE, assertThrows( ExecutionException.class, failed::get ).getCause() );

		// A new caller starts a new fetch instead of joining the failed one
		final Call next = call( generator, context );
		generator.awaitFetches( 2 );
		generator.complete( generator.fetches.get( 1 ), 1 );
		assertEquals( 1L, next.get() );
	}

	@Test
	public void exceptionThrownByNextHiValueIsRetriedWithTheConnectionOfEachCaller() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final Context context = newContext();
		generator.failSynchronously = true;
		final List<Call> calls = calls( generator, context, 10 );
		for ( Call call : calls ) {
			assertSame( FAILURE, assertThrows( ExecutionException.class, call::get ).getCause() );
		}
		assertEquals( calls, generator.connections );

		generator.failSynchronously = false;
		final Call next = call( generator, context );
		generator.awaitFetches( 1 );
		generator.complete( generator.fetches.get( 0 ), 1 );
		assertEquals( 1L, next.get() );
	}

	@Test
	public void fetchThatCannotBeScheduledIsNotJoined() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		// Without a Vert.x context, the request for a new block can't be scheduled
		final CompletableFuture<Long> outsideContext = generator.generate( new Call(), null ).toCompletableFuture();
		assertThrows( ExecutionException.class, () -> outsideContext.get( TIMEOUT_SECONDS, SECONDS ) );
		assertTrue( generator.connections.isEmpty() );

		final Call next = call( generator, newContext() );
		generator.awaitFetches( 1 );
		generator.complete( generator.fetches.get( 0 ), 1 );
		assertEquals( 1L, next.get() );
	}

	@Test
	public void callersCompleteOnTheirOwnContext() throws Exception {
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, false );
		final Context first = newContext();
		final Context second = newContext();
		final List<Call> firstCalls = calls( generator, first, 10 );
		final List<Call> secondCalls = calls( generator, second, 10 );
		generator.awaitFetches( 1 );

		// Completed from a thread that is not an event loop
		generator.complete( generator.fetches.get( 0 ), 1 );

		final Set<Long> ids = new HashSet<>( ids( firstCalls ) );
		ids.addAll( ids( secondCalls ) );
		assertEquals( 20, ids.size() );
		for ( Call call : firstCalls ) {
			assertSame( first, call.completionContext );
		}
		for ( Call call : secondCalls ) {
			assertSame( second, call.completionContext );
		}
	}

	@Test
	public void blockSizeOfOneUsesOneFetchForEachCaller() throws Exception {
		final FakeGenerator generator = new FakeGenerator( 1, false );
		final Context context = newContext();
		final List<Call> calls = calls( generator, context, 3 );
		generator.awaitFetches( 3 );
		assertEquals( 3, generator.maxInFlight.get() );

		generator.complete( generator.fetches.get( 0 ), 7 );
		generator.complete( generator.fetches.get( 1 ), 8 );
		generator.complete( generator.fetches.get( 2 ), 9 );
		assertEquals( List.of( 7L, 8L, 9L ), ids( calls ) );

		final Call next = call( generator, context );
		generator.awaitFetches( 4 );
		generator.complete( generator.fetches.get( 3 ), 10 );
		assertEquals( 10L, next.get() );
	}

	@Test
	public void sustainedLoadOnMultipleContextsDoesNotDuplicateIds() throws Exception {
		final int callers = 120;
		final int rounds = 25;
		final int contexts = 4;
		final FakeGenerator generator = new FakeGenerator( BLOCK_SIZE, true );
		final List<Context> eventLoops = new ArrayList<>();
		for ( int i = 0; i < contexts; i++ ) {
			eventLoops.add( newContext() );
		}

		final List<Long> ids = Collections.synchronizedList( new ArrayList<>() );
		final List<CompletableFuture<Void>> done = new ArrayList<>();
		for ( int i = 0; i < callers; i++ ) {
			final Context context = eventLoops.get( i % contexts );
			final CompletableFuture<Void> finished = new CompletableFuture<>();
			done.add( finished );
			context.runOnContext( v -> nextId( generator, context, rounds, ids, finished ) );
		}
		CompletableFuture.allOf( done.toArray( new CompletableFuture[0] ) ).get( TIMEOUT_SECONDS, TimeUnit.SECONDS );

		assertEquals( callers * rounds, ids.size() );
		assertEquals( callers * rounds, new HashSet<>( ids ).size() );
		// A fetch has at most blockSize waiters and every caller only waits for one fetch at a time
		assertTrue( generator.maxInFlight.get() <= (int) Math.ceil( callers / (double) BLOCK_SIZE ) );
		assertTrue( generator.fetches.size() >= (int) Math.ceil( ids.size() / (double) BLOCK_SIZE ) );
	}

	private static void nextId(FakeGenerator generator, Context context, int remaining, List<Long> ids, CompletableFuture<Void> finished) {
		if ( remaining == 0 ) {
			finished.complete( null );
			return;
		}
		generator.generate( CONNECTION_SUPPLIER, null ).whenComplete( (id, throwable) -> {
			if ( throwable != null ) {
				finished.completeExceptionally( throwable );
				return;
			}
			ids.add( id );
			nextId( generator, context, remaining - 1, ids, finished );
		} );
	}
}
