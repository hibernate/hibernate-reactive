/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import java.util.concurrent.CompletionStage;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcSelect;
import org.hibernate.sql.exec.spi.LoadedValuesCollector;

/**
 * Reactive version of {@link JdbcSelect}
 */
public interface ReactiveJdbcSelect extends JdbcSelect {

	CompletionStage<Void> reactivePerformPreActions(ReactiveConnection connection, ExecutionContext executionContext);

	CompletionStage<Void> reactivePerformPostActions(
			boolean succeeded,
			ReactiveConnection connection,
			ExecutionContext executionContext,
			LoadedValuesCollector loadedValuesCollector);
}
