/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.action.spi.Executable;

/**
 * An operation that is scheduled for later non-blocking
 * execution in an {@link ReactiveActionQueue}. Reactive counterpart
 * to {@link Executable}.
 */
public interface ReactiveExecutable extends Executable, Serializable {
	CompletionStage<Void> reactiveExecute();
}
