/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.vertx.core.Context;
import io.vertx.sqlclient.Pool;

/**
 * A single Vert.x Pool instance should only be used within its context;
 * the purpose of this class is to maintain an efficient set of ThreadLocal
 * instances so that each Vert.x context can use an independent one,
 * and track them all to not leak resources on shutdown.
 *
 * @param <PoolType> could be useful to pool database specific types of connection pools.
 * @author Sanne Grinovero
 */
final class ThreadLocalPoolManager<PoolType extends Pool> {

	private static final Logger log = Logger.getLogger( ThreadLocalPoolManager.class);

	private final AtomicReference<ThreadLocalPoolSet> poolset = new AtomicReference<>(new ThreadLocalPoolSet());
	private final Supplier<PoolType> poolSupplier;

	public ThreadLocalPoolManager(Supplier<PoolType> poolSupplier) {
		Objects.requireNonNull( poolSupplier );
		this.poolSupplier = poolSupplier;
	}

	private PoolType pool() {
		//We re-try to be nice on an extremely unlikely race condition:
		//a pool being requested while a close has been requested in parallel.
		//A loop of 3 attempts should be more than enough as the close
		//operation can't happen more than once.
		//N.B. while we're graciously handling this race, we expect the
		//external integration to avoid this scenario.
		for (int i = 0; i < 3; i++) {
			final ThreadLocalPoolSet currentConnections = poolset.get();
			PoolType p = currentConnections.getPool();
			if (p != null)
				return p;
		}
		throw new IllegalStateException("Multiple attempts to reopen a new pool on a closed instance: aborting");
	}

	private PoolType createThreadLocalPool() {
		return poolSupplier.get();
	}

	/**
	 * This is a bit weird because it works on all ThreadLocal pools, but it's only
	 * called from a single thread, when doing shutdown, and needs to close all the
	 * pools and reinitialise the thread local so that all newly created pools after
	 * the restart will start with an empty thread local instead of a closed one.
	 * N.B. while we take care of the pool to behave as best as we can,
	 * it's responsibility of the user of the returned pools to not use them
	 * while a close is being requested.
	 */
	public void close() {
		// close all the thread-local pools, then discard the current ThreadLocal pool.
		final ThreadLocalPoolSet previousPool = poolset.getAndSet(new ThreadLocalPoolSet());
		previousPool.close();
	}

	public PoolType getOrStartPool() {
		//First, check that the requester is running within the EventLoop:
		if ( !Context.isOnEventLoopThread() ) {
			//Not using the InternalStateAssertions here as this check is more critical; need to ensure all production code actually adheres to this constraint.
			//On top of correctness (accessing the pool from a non-eventloop thread exposes us to race conditions), we also don't want to the ThreadLocal
			//to store more Pool references than the configured Vert.x threads.
			throw new IllegalStateException(
					"A Reactive SQL Client Pool can only be used from within a Vert.x context. You are using Hibernate Reactive within a thread which is not a Vert.x event loop." );
		}
		return pool();
	}

	private class ThreadLocalPoolSet {
		final List<Pool> threadLocalPools = new ArrayList<>();
		final ThreadLocal<PoolType> threadLocal = new ThreadLocal<>();
		final StampedLock stampedLock = new StampedLock();
		boolean isOpen = true;

		public PoolType getPool() {
			final long optimisticRead = stampedLock.tryOptimisticRead();
			if (isOpen == false) {
				//Let the caller re-try on a different instance
				return null;
			}
			PoolType ret = threadLocal.get();
			if (ret != null) {
				if (stampedLock.validate(optimisticRead)) {
					return ret;
				} else {
					//On invalid optimisticRead stamp, it means this pool instance was closed:
					//let the caller re-try on a different instance
					return null;
				}
			} else {
				//Now acquire an exclusive readlock:
				final long readLock = stampedLock.tryConvertToReadLock(optimisticRead);
				//Again, on failure the pool was closed, return null in such case.
				if (readLock == 0)
					return null;
				//else, we own the exclusive read lock and can now enter our slow path:
				try {
					log.debugf("Making pool for thread: %s", Thread.currentThread());
					ret = createThreadLocalPool();
					synchronized (threadLocalPools) {
						threadLocalPools.add(ret);
					}
					threadLocal.set(ret);
					return ret;
				} finally {
					stampedLock.unlockRead(readLock);
				}
			}
		}

		public void close() {
			final long lock = stampedLock.writeLock();
			try {
				isOpen = false;
				//While this synchronized block might take a while as we have to close all
				//pool instances, it shouldn't block the getPool method as contention is
				//prevented by the exclusive stamped lock.
				synchronized (threadLocalPools) {
					for (Pool pool : threadLocalPools) {
						log.debugf("Closing thread-scoped SQL Client pool instance: %s", pool);
						pool.close();
					}
				}
			} finally {
				stampedLock.unlockWrite(lock);
			}
		}
	}

}
