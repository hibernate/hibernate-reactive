/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.tuple;

import org.hibernate.Session;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.tuple.ValueGenerator;

import io.smallrye.mutiny.Uni;

/**
 * A reactive {@link ValueGenerator} that gives access to a {@link Mutiny.Session}.
 * <p>
 * This class needs to extend {@link ValueGenerator} but the non reactive method is ignored by
 * Hibernate Reactive and it will throw an {@link UnsupportedOperationException} if called.
 * </p>
 *
 * @see StageValueGenerator
 */
public abstract class MutinyValueGenerator<T> implements ValueGenerator<T> {

	@Override
	public final T generateValue(Session session, Object owner) {
		throw new UnsupportedOperationException( "Use generateValue(Mutiny.Session, Object) instead" );
	}

	public abstract Uni<T> generateValue(Mutiny.Session session, Object owner);
}
