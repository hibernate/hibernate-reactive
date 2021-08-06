/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import java.util.Objects;

import org.hibernate.reactive.context.Context;

/**
 * Implementation of {@link Context.Key} suitable for storing
 * Session, StatelessSession or other type instances in the
 * Vert.x context.
 * @param <T> the type of object being stored in the Context.
 */
public final class BaseKey<T> implements Context.Key<T> {

	private final Class<T> type;
	private final String id;
	private final int hash;

	public BaseKey(Class<T> type, String id) {
		Objects.requireNonNull( type );
		Objects.requireNonNull( id );
		this.type = type;
		this.id = id;
		this.hash = id.hashCode() * 31 + type.hashCode();
	}

	@Override
	public boolean equals(Object object) {
		if ( this == object ) {
			return true;
		}
		if ( !( object instanceof BaseKey ) ) {
			return false;
		}
		BaseKey<?> key = (BaseKey<?>) object;
		return Objects.equals( id, key.id )
				&& Objects.equals( type, key.type );
	}

	@Override
	public int hashCode() {
		return hash;
	}
}
