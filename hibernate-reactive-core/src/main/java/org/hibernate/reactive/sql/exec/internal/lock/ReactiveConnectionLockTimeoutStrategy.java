/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.dialect.lock.spi.ConnectionLockTimeoutStrategy;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.pool.ReactiveConnection;

import jakarta.persistence.Timeout;
import java.util.concurrent.CompletionStage;

/**
 * Reactive version of {@link ConnectionLockTimeoutStrategy}
 */
public interface ReactiveConnectionLockTimeoutStrategy extends ConnectionLockTimeoutStrategy {

	default CompletionStage<Timeout> getReactiveLockTimeout(
			ReactiveConnection connection,
			SessionFactoryImplementor factory) {
		throw new UnsupportedOperationException( "Lock timeout on the connection is not supported" );
	}

	default CompletionStage<Void> setReactiveLockTimeout(
			Timeout timeout,
			ReactiveConnection connection,
			SessionFactoryImplementor factory) {
		throw new UnsupportedOperationException( "Lock timeout on the connection is not supported" );
	}
}
