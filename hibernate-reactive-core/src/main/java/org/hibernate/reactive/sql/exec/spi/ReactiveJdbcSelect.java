/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import org.hibernate.Incubating;
import org.hibernate.sql.exec.spi.JdbcSelect;

/**
 * Reactive version of {@link JdbcSelect}
 */
@Incubating(since = "5.0")
public interface ReactiveJdbcSelect extends JdbcSelect {

	/**
	 * Creates the actions of one execution, to use for both its pre-actions and its post-actions.
	 * The actions of a {@code JdbcSelect} can be shared by concurrent executions, for example through the
	 * query plan cache, so an action that keeps state creates a new instance for each execution.
	 *
	 * @see JdbcSelect#createActionExecution()
	 */
	ReactiveJdbcActionExecution createReactiveActionExecution();
}
