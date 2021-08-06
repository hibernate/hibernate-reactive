/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import java.util.Objects;

import org.hibernate.reactive.context.Context;

/**
 * Implementation of {@link Context.Key} suitable for the
 * use case with multi-tenancy.
 * This wraps the {@link BaseKey} of the same type, allowing
 * instance reuse for efficiency.
 * @param <T> the type of object being stored in the Context.
 */
public final class MultitenantKey<T> implements Context.Key<T> {

	final BaseKey<T> base;
	final String tenantId;

	public MultitenantKey(BaseKey<T> base, String tenantId) {
		Objects.requireNonNull( base );
		Objects.requireNonNull( tenantId );
		this.base = base;
		this.tenantId = tenantId;
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( !( o instanceof MultitenantKey ) ) {
			return false;
		}

		MultitenantKey<?> that = (MultitenantKey<?>) o;

		if ( !base.equals( that.base ) ) {
			return false;
		}
		return tenantId.equals( that.tenantId );
	}

	@Override
	public int hashCode() {
		int result = base.hashCode();
		result = 31 * result + tenantId.hashCode();
		return result;
	}
}
