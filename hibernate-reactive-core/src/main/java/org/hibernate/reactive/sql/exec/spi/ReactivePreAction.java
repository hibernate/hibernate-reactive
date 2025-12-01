/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.spi;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.PreAction;

import java.util.concurrent.CompletionStage;

/**
 * Reactive version of {@link PreAction}
 */
public interface ReactivePreAction extends PreAction {


	CompletionStage<Void> reactivePerformPreAction(ReactiveConnection connection, ExecutionContext executionContext);
}
