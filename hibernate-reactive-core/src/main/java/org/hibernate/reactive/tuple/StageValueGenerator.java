/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.tuple;

import java.util.concurrent.CompletionStage;

import org.hibernate.Session;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.tuple.ValueGenerator;

/**
 * A reactive {@link ValueGenerator} that gives access to a {@link Stage.Session}.
 * <p>
 * This interface needs to extend {@link ValueGenerator} but the non reactive method is ignored by
 * Hibernate Reactive and, unless the default implementation is overridden, it will throw an exception
 * when called.
 * </p>
 *
 * @see MutinyValueGenerator
 */
public interface StageValueGenerator<T> extends ValueGenerator<T> {

	@Override
	default T generateValue(Session session, Object owner) {
		throw new UnsupportedOperationException( "Use generateValue(Stage.Session, Object) instead" );
	}

	CompletionStage<T> generateValue(Stage.Session session, Object owner);
}
