/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.internal;

import java.util.function.Function;

/**
 * A transaction that can execute transactional work. Implemented by the
 * inner {@code InternalTransaction} classes in each session implementation,
 * allowing {@link CurrentTransaction#execute} to invoke the transaction
 * without knowing its concrete type.
 *
 * @param <T> the transaction interface type ({@link org.hibernate.reactive.mutiny.Mutiny.Transaction}
 *            or {@link org.hibernate.reactive.stage.Stage.Transaction})
 * @param <R> the result type of the transactional work
 */
public interface ExecutableTransaction<T, R> {
	R execute(Function<T, R> work, Runnable cleanup);
}
