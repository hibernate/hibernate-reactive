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
 * This class needs to extend {@link ValueGenerator} but the non reactive method is ignored by
 * Hibernate Reactive and it will throw an {@link UnsupportedOperationException} if called.
 * </p>
 *
 * @see MutinyValueGenerator
 */
public abstract class StageValueGenerator<T> implements ValueGenerator<T> {

	@Override
	public final T generateValue(Session session, Object owner) {
		throw new UnsupportedOperationException( "Use generateValue(Stage.Session, Object) instead" );
	}

	public abstract CompletionStage<T> generateValue(Stage.Session session, Object owner);
}
