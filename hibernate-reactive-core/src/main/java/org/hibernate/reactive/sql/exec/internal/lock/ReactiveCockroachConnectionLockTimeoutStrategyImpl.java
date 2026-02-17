/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnection;

import jakarta.persistence.Timeout;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.util.concurrent.CompletionStage;

import static org.hibernate.Timeouts.WAIT_FOREVER;

public class ReactiveCockroachConnectionLockTimeoutStrategyImpl
		implements ReactiveConnectionLockTimeoutStrategy {

	public static final ReactiveCockroachConnectionLockTimeoutStrategyImpl INSTANCE = new ReactiveCockroachConnectionLockTimeoutStrategyImpl();

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	public Timeout getLockTimeout(Connection connection, SessionFactoryImplementor factory) {
		throw LOG.nonReactiveMethodCall( "getReactiveLockTimeout()" );
	}

	@Override
	public void setLockTimeout(Timeout timeout, Connection connection, SessionFactoryImplementor factory) {
		throw LOG.nonReactiveMethodCall( "setReactiveLockTimeout()" );
	}

	@Override
	public Level getSupportedLevel() {
		return ConnectionLockTimeoutStrategy.Level.SUPPORTED;
	}

	@Override
	public CompletionStage<Timeout> getReactiveLockTimeout(
			ReactiveConnection connection,
			SessionFactoryImplementor factory) {
		return LockHelper.getLockTimeout(
				"show lock_timeout",
				ReactiveCockroachConnectionLockTimeoutStrategyImpl::getTimeout,
				connection
		);
	}

	private static Timeout getTimeout(ReactiveConnection.Result resultSet) {
		int millis = Integer.parseInt( (String) resultSet.next()[0] );
		return millis == 0 ? WAIT_FOREVER : Timeout.milliseconds( millis );
	}

	@Override
	public CompletionStage<Void> setReactiveLockTimeout(
			Timeout timeout,
			ReactiveConnection connection,
			SessionFactoryImplementor factory) {
		return ReactivePostgreSQLConnectionLockTimeoutStrategyImpl.INSTANCE.setReactiveLockTimeout( timeout, connection, factory );
	}

}
