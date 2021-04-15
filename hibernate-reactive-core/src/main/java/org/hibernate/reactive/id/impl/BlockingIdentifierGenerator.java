/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id.impl;

import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A {@link ReactiveIdentifierGenerator} which uses the database to allocate
 * blocks of ids. A block is identified by its "hi" value (the first id in
 * the block). While a new block is being allocated, concurrent streams wait
 * without blocking.
 *
 * @author Gavin King
 */
public abstract class BlockingIdentifierGenerator implements ReactiveIdentifierGenerator<Long> {

    /**
     * The block size (the number of "lo" values for each "hi" value)
     */
    protected abstract int getBlockSize();

    /**
     * Allocate a new block, by obtaining the next "hi" value from the database
     */
    protected abstract CompletionStage<Long> nextHiValue(ReactiveConnectionSupplier session);

    private int loValue;
    private long hiValue;

    private volatile List<Runnable> queue = null;

    protected synchronized long next() {
        return loValue>0 && loValue<getBlockSize()
                ? hiValue + loValue++
                : -1; //flag value indicating that we need to hit db
    }

    protected synchronized long next(long hi) {
        hiValue = hi;
        loValue = 1;
        return hi;
    }

    @Override
    public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
        if ( getBlockSize()<=1 ) {
            //special case where we're not using blocking at all
            return nextHiValue(session);
        }

        long local = next();
        if ( local >= 0 ) {
            // We don't need to update or initialize the hi
            // value in the table, so just increment the lo
            // value and return the next id in the block
            return completedFuture(local);
        }
        else {
            synchronized (this) {
                CompletableFuture<Long> result = new CompletableFuture<>();
                if (queue == null) {
                    // make a queue for any concurrent streams
                    queue = new ArrayList<>();
                    // go off and fetch the next hi value from db
                    nextHiValue(session).thenAccept( id -> {
//						Vertx.currentContext().runOnContext(v -> {
                        List<Runnable> list;
                        synchronized (this) {
                            // clone ref to the queue
                            list = queue;
                            queue = null;
                            // use the fetched hi value in this stream
                            result.complete( next(id) );
                        }
                        // send waiting streams back to try again
                        list.forEach(Runnable::run);
//						} );
                    } );
                }
                else {
                    // wait for the concurrent fetch to complete
                    // note that we carefully capture the right session,entity here!
                    queue.add( () -> generate(session, entity).thenAccept(result::complete) );
                }
                return result;
            }
        }
    }
}
