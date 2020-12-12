/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import io.vertx.ext.unit.TestContext;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.ProxyConnectionTestHelper.ReactiveConnectionPoolMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Low-level test to prevent "single-thread-connection-race" in {@link ProxyConnection}.
 * <p>
 * See {@link org.hibernate.reactive.MutinySessionTest#reactiveProxyConnectionRace(TestContext)} and
 * <a href="https://github.com/hibernate/hibernate-reactive/issues/475">#475</a>.
 * </p>
 */
public class ProxyConnectionRaceTest {
    private ExecutorService executor;

    /**
     * Concurrency for "hammering" test - at least 4 threads, max=availableProcessors.
     */
    private static final int parallelism = Math.max(4, Runtime.getRuntime().availableProcessors());

    /**
     * See parallelism multiplicators in {@link #concurrentWithConnectionN(int)}
     */
    private static final int threadPoolSize = parallelism * 3 ;

    @Before
    public void setup() {
        executor = Executors.newFixedThreadPool(threadPoolSize);
    }

    @After
    public void teardown() throws Exception {
        try {
            executor.shutdownNow();
            assertTrue(
                    "At least one thread keeps spinning around, not good",
                    executor.awaitTermination(30, TimeUnit.SECONDS));
        } finally {
            executor = null;
        }
    }

    /**
     * Test case that exercises the code paths in {@link ProxyConnection} for a Quarkus/Vert.x code
     * snippet like this:
     *
     * <code><pre>
     * import io.smallrye.mutiny.Uni;
     * import org.hibernate.reactive.mutiny.Mutiny;
     *
     * public class SomeRestEndpoint {
     *     &#64;Inject
     *     Mutiny.Session mutinySession;
     *
     *     &#64;GET
     *     public Uni&lt;String&gt; doSomeTransactionalStuff() {
     *         Uni&lt;String&gt; work = mutinySession.find(SomeEntity.class, 42L)
     *             .map(e -> e.getColumnValue());
     *
     *         return mutinySession.withTransaction(tx -> work);
     *     }
     * }
     * </pre></code>
     */
    @Test
    public void noRace() throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Object> delay = new CompletableFuture<>();
        ReactiveConnectionPoolMock reactiveConnectionPool = new ReactiveConnectionPoolMock(
                () -> CompletableFuture.completedFuture(new SqlClientConnection(null, null, null, false)),
                delay);

        ProxyConnection proxyConnection = ProxyConnection.newInstance(reactiveConnectionPool);

        // Simulate the `mutinySession.find` which triggers a 'ProxyConnection.withConnection()'
        // Just let it return some string when it could actually "do" something with the connection from the pool.
        String result1 = "piece-of-work #1";
        CompletionStage<String> cs1 = proxyConnection.withConnection(
                reactiveConnection -> CompletableFuture.completedFuture(result1));
        CompletableFuture<String> cf1 = cs1.toCompletableFuture(); // this is basically just a cast to CF

        // Work cannot be completed yet, because the connection pool hasn't returned a connection yet.
        assertFalse(cf1.isDone());

        // Simulate the 'mutinySession.withTransaction' which triggers another 'ProxyConnection.withConnection()'
        // Just let it return some string when it could actually "do" something with the connection from the pool.
        String result2 = "piece-of-work #2";
        CompletionStage<String> cs2 = proxyConnection.withConnection(
                reactiveConnection -> CompletableFuture.completedFuture(result2));
        CompletableFuture<String> cf2 = cs2.toCompletableFuture(); // this is basically just a cast to CF

        // Work cannot be completed yet, because the connection pool hasn't returned a connection yet.
        assertFalse(cf2.isDone());

        // Simulate that the connection from the pool is now available
        delay.complete("");
        proxyConnection.openConnection();

        // It shouldn't take long for the CF to finish - it will eventually complete.
        // There will be some time required as stuff in this test is ran asynchronously.
        // Overloaded CI systems can cause tests to become really slow, so 30s is
        // hopefully long enough to let this test not become flaky. Crossing fingers.
        String response1 = cf1.get(30, TimeUnit.SECONDS);
        String response2 = cf2.get(30, TimeUnit.SECONDS);

        assertEquals(result1, response1);
        assertEquals(result2, response2);
    }

    /**
     * Make sure that running many concurrent calls to {@link ProxyConnection#withConnection(Function)}
     * doesn't break the functionality, albeit it's not good/recommended practice to do that.
     * <p>
     * However, using {@link org.hibernate.reactive.mutiny.Mutiny.Session} via
     * {@link io.smallrye.mutiny.Multi} can lead to exactly this situation. This test however only
     * exercises the {@link ProxyConnection#withConnection(Function)} part.
     * <p>
     * This variant of the test simulates the situation that the connection-pool supplies the
     * connection after all {@link ProxyConnection#withConnection(Function)} invocations have
     * issued. The other variants {@link #concurrentWithConnection1Iterations()} and
     * {@link #concurrentWithConnection2Iterations()} let the connection-pool return the
     * connection after 1 respective 2 invocations of {@link ProxyConnection#withConnection(Function)}.
     */
    @Test
    public void concurrentWithConnectionAllIterations() throws Exception {
        concurrentWithConnectionN(parallelism);
    }

    /**
     * See {@link #concurrentWithConnectionAllIterations()}.
     */
    @Test
    public void concurrentWithConnection2Iterations() throws Exception {
        concurrentWithConnectionN(2);
    }

    /**
     * See {@link #concurrentWithConnectionAllIterations()}.
     */
    @Test
    public void concurrentWithConnection1Iterations() throws Exception {
        concurrentWithConnectionN(1);
    }

    public void concurrentWithConnectionN(int nWithConnectionCalls) throws Exception {
        for (int i = 0; i < 5; i++) {
            concurrentWithConnectionN(nWithConnectionCalls, parallelism);
            concurrentWithConnectionN(nWithConnectionCalls, parallelism * 2);
            concurrentWithConnectionN(nWithConnectionCalls, parallelism * 3);
        }
    }

    public void concurrentWithConnectionN(int nWithConnectionCalls, int workers) throws Exception {
        CompletableFuture<Object> delay = new CompletableFuture<>();
        ReactiveConnectionPoolMock reactiveConnectionPool = new ReactiveConnectionPoolMock(
                () -> CompletableFuture.completedFuture(new SqlClientConnection(null, null, null, false)),
                delay);

        ProxyConnection proxyConnection = ProxyConnection.newInstance(reactiveConnectionPool);

        CountDownLatch withConnectionReady = new CountDownLatch(workers);
        CountDownLatch withConnectionLatch = new CountDownLatch(1);

        // The CompletionStage by worker# instances as returned from ProxyConnection.withConnection
        ConcurrentHashMap<Integer, CompletionStage<Integer>> scheduledStages = new ConcurrentHashMap<>();
        // The ReactiveConnection instance by worker#
        ConcurrentHashMap<Integer, ReactiveConnection> completedStages = new ConcurrentHashMap<>();

        // counter to adjust when the connection is simulated to be returned from the connection-pool
        AtomicInteger numWithConnectionDone = new AtomicInteger();

        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            int num = i; // effectively final i
            futures.add(CompletableFuture.runAsync(() -> {
                // tell the "main" thread that this worker is ready
                withConnectionReady.countDown();

                // wait for the main thread to let this worker start
                try {
                    assertTrue(withConnectionLatch.await(30, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e); // just bark
                }

                // jump into 'withConnection'
                CompletionStage<Integer> cs = proxyConnection.withConnection(
                        conn -> CompletableFuture.supplyAsync(
                                () -> {
                                    completedStages.put(num, conn);
                                    return num;
                                },
                                executor));

                // check whether the connection shall be returned from the connection-pool
                if (numWithConnectionDone.incrementAndGet() == nWithConnectionCalls) {
                    delay.complete("");
                    proxyConnection.openConnection();
                }

                scheduledStages.put(num, cs);
            }, executor));
        }

        // Wait until all workers are ready...
        assertTrue(withConnectionReady.await(30, TimeUnit.SECONDS));

        // Trigger all workers to call ProxyConnection.withConnection() somewhat simultaneously
        withConnectionLatch.countDown();

        // make sure our workers have finished
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(30, TimeUnit.SECONDS);

        // check that the CompletionStages returned by withConnection() did complete
        for (int i = 0; i < workers; i++) {
            CompletionStage<Integer> cs = scheduledStages.get(i);
            assertNotNull(cs);
            assertEquals(Integer.valueOf(i), cs.toCompletableFuture().get(5, TimeUnit.SECONDS));
        }

        // verify that all CompletionStages passed into ProxyConnection.withConnection() did execute
        Set<Integer> expected0toParallelism = IntStream.range(0, workers).boxed().collect(Collectors.toSet());
        assertEquals(expected0toParallelism, completedStages.keySet());

        // verify that it's the same ReactiveConnection instance passed into all 'operations'
        Iterator<ReactiveConnection> iCompleted = completedStages.values().iterator();
        ReactiveConnection conn = iCompleted.next();
        assertNotNull(conn);
        while (iCompleted.hasNext())
            assertSame(conn, iCompleted.next());
    }

    /**
     * Verify that the real connections acquired via {@link ProxyConnection} and held in
     * {@link ProxyConnection#connection} are properly closed in overload situations.
     * "Overload situations" means, that the connection pool cannot provide enough connections
     * (for whatever reasons) to satisfy all connection requests.
     * <p>
     * Example situation: the database connection pool is limited to max 1 connections,
     * a lot of (long running) application requests pile up and in consequence the system
     * is not able to execute all these requests within a configured timeout, so most
     * requests get aborted via some timeout handling mechanism.
     * <p>
     * These situations are properly handled as long as the calling code properly issues
     * a {@link ProxyConnection#close()}, the logic in
     * {@link ProxyConnection#connectionFromPool(ReactiveConnection)} ensures, that a
     * database connection for a closed {@link ProxyConnection} is immediately closed and
     * therefore properly returned to the pool.
     */
    @Test
    public void overloadSituation() {

        // Whether the pool has given the ProxyConnection a "database" connection
        AtomicBoolean connectionAcquired = new AtomicBoolean();

        // Whether the pool's connection's close() method has been called
        AtomicBoolean closeCalled = new AtomicBoolean();

        // Whether the operation passed into ProxyConnection#withConnection got called
        AtomicBoolean receivedConnection = new AtomicBoolean();

        CompletableFuture<Object> delay = new CompletableFuture<>();
        ReactiveConnectionPoolMock reactiveConnectionPool = new ReactiveConnectionPoolMock(
                () -> {
                    connectionAcquired.set(true);
                    return CompletableFuture.completedFuture(new SqlClientConnection(null, null, null, false) {
                        @Override
                        public void close() {
                            closeCalled.set(true);
                        }
                    });
                },
                delay);

        assertFalse(connectionAcquired.get());
        assertFalse(closeCalled.get());

        ProxyConnection proxyConnection = ProxyConnection.newInstance(reactiveConnectionPool);

        CompletableFuture<Object> requestStage = proxyConnection.withConnection(x -> {
            receivedConnection.set(true);
            return CompletableFuture.completedFuture(null);
        }).toCompletableFuture();

        // Sanity assertions - nothing should have happened yet...
        assertFalse(receivedConnection.get());
        assertFalse(connectionAcquired.get());
        assertFalse(closeCalled.get());

        CompletableFuture<ReactiveConnection> openStage = proxyConnection.openConnection().toCompletableFuture();

        // The openConnection() just starts the acquire-connection process, but nothing's returned,
        // because the pool hasn't returned a connection yet
        assertFalse(requestStage.isDone());
        assertFalse(openStage.isDone());

        // Eagerly close the ProxyConnection (i.e. what would happen if the application request
        // closes the surrounding Hibernate session).
        proxyConnection.close();

        // no connection should have been received
        assertFalse(receivedConnection.get());
        // Both returned stages must have completed exceptionally
        assertTrue(requestStage.isCompletedExceptionally());
        assertTrue(openStage.isCompletedExceptionally());

        // Still no connection acquired, also no call to the connection from the pool
        assertFalse(connectionAcquired.get());
        // The pool's connection's close() method hasn't been called
        assertFalse(closeCalled.get());

        // Let the pool return a connection
        delay.complete("");

        // no connection should have been received
        assertFalse(receivedConnection.get());
        // The acquire callback has been triggered (from the pool)
        assertTrue(connectionAcquired.get());
        // And that should have called close() on the connection from the pool
        assertTrue(closeCalled.get());
    }

    /**
     * Normal ProxyConnection lifecycle (the boring good-case).
     *
     * <ol>
     *     <li>Application opens Session...{@link ProxyConnection}</li>
     *     <li>Demands get registered via {@link ProxyConnection#withConnection(Function)}</li>
     *     <li>Subscription gets triggered via {@link ProxyConnection#openConnection()}</li>
     *     <li>(stuff works)</li>
     *     <li>Application closes Session...{@link ProxyConnection} -> connection gets returned to the pool</li>
     * </ol>
     */
    @Test
    public void normal() {

        // Whether the pool has given the ProxyConnection a "database" connection
        AtomicBoolean connectionAcquired = new AtomicBoolean();

        // Whether the pool's connection's close() method has been called
        AtomicBoolean closeCalled = new AtomicBoolean();

        // Whether the operation passed into ProxyConnection#withConnection got called
        AtomicBoolean receivedConnection = new AtomicBoolean();

        CompletableFuture<Object> delay = new CompletableFuture<>();
        ReactiveConnectionPoolMock reactiveConnectionPool = new ReactiveConnectionPoolMock(
                () -> {
                    connectionAcquired.set(true);
                    return CompletableFuture.completedFuture(new SqlClientConnection(null, null, null, false) {
                        @Override
                        public void close() {
                            closeCalled.set(true);
                        }
                    });
                },
                delay);

        assertFalse(connectionAcquired.get());
        assertFalse(closeCalled.get());

        ProxyConnection proxyConnection = ProxyConnection.newInstance(reactiveConnectionPool);

        CompletableFuture<Object> requestStage = proxyConnection.withConnection(x -> {
            receivedConnection.set(true);
            return CompletableFuture.completedFuture(null);
        }).toCompletableFuture();

        // Sanity assertions - nothing should have happened yet...
        assertFalse(receivedConnection.get());
        assertFalse(connectionAcquired.get());
        assertFalse(closeCalled.get());

        CompletableFuture<ReactiveConnection> openStage = proxyConnection.openConnection().toCompletableFuture();

        // The openConnection() just starts the acquire-connection process, but nothing's returned,
        // because the pool hasn't returned a connection yet
        assertFalse(requestStage.isDone());
        assertFalse(openStage.isDone());

        // Let the pool return a connection
        delay.complete("");

        // a connection should have been received
        assertTrue(receivedConnection.get());
        // Both returned stages must have completed exceptionally
        assertTrue(requestStage.isDone() && !requestStage.isCompletedExceptionally());
        assertTrue(openStage.isDone() && !openStage.isCompletedExceptionally());

        // Still no connection acquired, also no call to the connection from the pool
        assertTrue(connectionAcquired.get());
        // The pool's connection's close() method hasn't been called
        assertFalse(closeCalled.get());

        // Eagerly close the ProxyConnection (i.e. what would happen if the application request
        // closes the surrounding Hibernate session).
        proxyConnection.close();

        // And that should have called close() on the connection from the pool
        assertTrue(closeCalled.get());
    }

}
