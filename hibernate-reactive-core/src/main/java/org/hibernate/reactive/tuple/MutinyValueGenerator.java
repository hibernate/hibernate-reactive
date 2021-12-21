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
 * This interface needs to extend {@link ValueGenerator} but Hibernate Reactive
 * ignores the method {@link MutinyValueGenerator#generateValue(Session, Object)}.
 * </p>
 *
 * @see StageValueGenerator
 */
public interface MutinyValueGenerator<T> extends ValueGenerator<T> {

	@Override
	default T generateValue(Session session, Object owner) {
		throw new UnsupportedOperationException( "Use generateValue(Mutiny.Session, Object) instead" );
	}

	Uni<T> generateValue(Mutiny.Session session, Object owner);
}
