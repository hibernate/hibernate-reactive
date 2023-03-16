/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.ExceptionConverterImpl;

/**
 * Handle exceptions and convert them following the logic used in Hibernate ORM.
 * <p>
 * It converts the exception to a {@link HibernateException} or a
 * {@link jakarta.persistence.PersistenceException}.
 *
 * @see org.hibernate.engine.spi.ExceptionConverter
 */
public class ReactiveExceptionConverter extends ExceptionConverterImpl {
	public ReactiveExceptionConverter(SharedSessionContractImplementor sharedSessionContract) {
		super( sharedSessionContract );
	}

	@Override
	public RuntimeException convert(RuntimeException e) {
		if ( !( e instanceof HibernateException ) ) {
			return super.convert( new HibernateException( e ) );
		}
		return super.convert( e );
	}
}
