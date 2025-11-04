/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.PostAction;

import java.util.concurrent.CompletionStage;

/**
 * Reactive version of {@link PostAction}
 */
public interface ReactivePostAction extends PostAction {

	CompletionStage<Void> reactivePerformReactivePostAction(
			ReactiveConnection jdbcConnection,
			ExecutionContext executionContext);
}
