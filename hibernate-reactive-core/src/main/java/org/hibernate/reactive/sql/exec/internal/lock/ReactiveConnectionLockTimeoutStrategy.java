/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
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

/**
 * Reactive version of {@link ConnectionLockTimeoutStrategy}
 */
public interface ReactiveConnectionLockTimeoutStrategy extends ConnectionLockTimeoutStrategy {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	@Override
	default Timeout getLockTimeout(Connection connection, SessionFactoryImplementor factory) {
		throw LOG.nonReactiveMethodCall( "getReactiveLockTimeout()" );
	}

	@Override
	default void setLockTimeout(Timeout timeout, Connection connection, SessionFactoryImplementor factory) {
		throw LOG.nonReactiveMethodCall( "setReactiveLockTimeout()" );
	}

	default CompletionStage<Timeout> getReactiveLockTimeout(ReactiveConnection connection, SessionFactoryImplementor factory){
		throw new UnsupportedOperationException( "Lock timeout on the connection is not supported" );
	}

	default CompletionStage<Void> setReactiveLockTimeout(
			Timeout timeout,
			ReactiveConnection connection,
			SessionFactoryImplementor factory){
		throw new UnsupportedOperationException( "Lock timeout on the connection is not supported" );
	}
}
