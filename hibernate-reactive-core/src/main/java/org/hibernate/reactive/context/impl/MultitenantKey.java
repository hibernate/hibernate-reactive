/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context.impl;

import java.util.Objects;

import org.hibernate.reactive.context.Context;

/**
 * Implementation of {@link Context.Key} suitable for use with
 * multi-tenancy. In a multi-tenant environment, anything we
 * store in the Vert.x context is keyed not only by its type,
 * but also by the tenant id.
 * <p>
 * An instance of this class wraps a {@link BaseKey} of the same
 * type, allowing instance reuse for efficiency.
 *
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
