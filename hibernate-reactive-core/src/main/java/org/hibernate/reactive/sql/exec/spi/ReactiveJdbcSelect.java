/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelect;

import java.util.concurrent.CompletionStage;

/**
 * Reactive version of {@link JdbcSelect}
 */
public interface ReactiveJdbcSelect extends JdbcSelect {



	CompletionStage<Void> reactivePerformPreActions(ReactiveConnection connection, ExecutionContext executionContext);

	CompletionStage<Void> reactivePerformPostActions(
			boolean succeeded,
			ReactiveConnection connection,
			ExecutionContext executionContext);
}
