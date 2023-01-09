/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.micrometer.core.instrument.Metrics;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.ext.unit.TestContext;
import org.jboss.logging.Logger;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static java.util.Arrays.stream;

public class CancelSignalTest extends BaseReactiveTest {
    private static final Logger LOG = Logger.getLogger( CancelSignalTest.class );

    @Override
    protected Collection<Class<?>> annotatedEntities() {
        return List.of(GuineaPig.class);
    }

    @Test
    public void cleanupConnectionWhenCancelSignal(TestContext context) {
        int executeSize = 10;
        ExecutorService withSessionExecutor = Executors.newFixedThreadPool(executeSize);
        ExecutorService cancelExecutor = Executors.newFixedThreadPool(executeSize);
        CountDownLatch firstSessionWaiter = new CountDownLatch(1);
        Queue<Cancellable> cancellableQueue = new ConcurrentLinkedQueue<>();

        CompletableFuture[] withSessionFutures = stream(new int[executeSize])
                .mapToObj(i ->
                        CompletableFuture.runAsync(
                                () -> {
                                    CountDownLatch countDownLatch = new CountDownLatch(1);
                                    String name = Thread.currentThread().getName();
                                    Cancellable cancellable = getMutinySessionFactory().withSession(s -> {
                                                try {
                                                    Thread.sleep(100);
                                                } catch (InterruptedException e) {
                                                    throw new RuntimeException(e);
                                                }
                                                firstSessionWaiter.countDown();
                                                return s.find(GuineaPig.class, 1);
                                            })
                                            .onTermination().invoke(countDownLatch::countDown)
                                            .subscribe()
                                            .with(item -> LOG.debug("end withSession."));
                                    cancellableQueue.add(cancellable);
                                    try {
                                        countDownLatch.await();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                withSessionExecutor
                        )
                )
                .toArray(CompletableFuture[]::new);

        CompletableFuture[] cancelFutures = stream(new int[executeSize])
                .mapToObj(i ->
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        firstSessionWaiter.await();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }

                                    Cancellable cancellable = cancellableQueue.poll();
                                    cancellable.cancel();
                                    try {
                                        Thread.sleep(500);
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                cancelExecutor
                        )
                )
                .toArray(CompletableFuture[]::new);

        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                Stream.concat(stream(withSessionFutures), stream(cancelFutures)).toArray(CompletableFuture[]::new)
        );

        test(context, voidCompletableFuture
                .thenAccept(x ->{
                    context.assertEquals(Metrics.globalRegistry.find("vertx.sql.queue.pending").gauge().value(), 0.0);
                })
        );

    }

    @Entity(name = "GuineaPig")
    @Table(name = "Pig")
    public static class GuineaPig {
        @Id
        private Integer id;
        private String name;

        public GuineaPig() {
        }

        public GuineaPig(Integer id, String name) {
            this.id = id;
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return id + ": " + name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GuineaPig guineaPig = (GuineaPig) o;
            return Objects.equals(name, guineaPig.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }
}
