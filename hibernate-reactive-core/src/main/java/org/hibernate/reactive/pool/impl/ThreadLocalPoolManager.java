/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;


import io.vertx.sqlclient.Pool;

/**
 * A single Vert.x Pool instance should only be used within its context;
 * the purpose of this class is to maintain an efficient set of ThreadLocal
 * instances so that each Vert.x context can use an independent one,
 * and track them all to not leak resources on shutdown.
 * When this class is created, no Pool instances are created: these need
 * to be created within the thread of the consumer, so all actual
 * connection creations are deferred to actual usage context.
 *
 * @param <PoolType> could be useful to pool database specific types of connection pools.
 * @author Sanne Grinovero
 */
final class ThreadLocalPoolManager<PoolType extends Pool> {

	//List of all opened pools. Access requires synchronization on the list instance.
	private final List<Pool> threadLocalPools = new ArrayList<>();

	//The pool instance for the current thread
	private final ThreadLocal<PoolType> threadLocal = new ThreadLocal<>();

	private final Supplier<PoolType> poolSupplier;

	private volatile boolean closed = false;

	public ThreadLocalPoolManager(Supplier<PoolType> poolSupplier) {
		Objects.requireNonNull( poolSupplier );
		this.poolSupplier = poolSupplier;
	}

	public PoolType getOrStartPool() {
		checkPoolIsOpen();
		PoolType pool = threadLocal.get();
		if ( pool == null ) {
			synchronized ( threadLocalPools ) {
				checkPoolIsOpen();
				pool = createThreadLocalPool();
				threadLocalPools.add( pool );
				threadLocal.set( pool );
			}
		}
		return pool;
	}

	private void checkPoolIsOpen() {
		if ( closed ) {
			throw new IllegalStateException("This Pool has been closed");
		}
	}

	private PoolType createThreadLocalPool() {
		return poolSupplier.get();
	}

	public void close() {
		synchronized ( threadLocalPools ) {
			this.closed = true;
			for ( Pool threadLocalPool : threadLocalPools ) {
				threadLocalPool.close();
			}
		}
	}

}
