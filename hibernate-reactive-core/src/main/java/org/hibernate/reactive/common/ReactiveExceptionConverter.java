/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.common;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.ExceptionConverterImpl;

/**
 * Handle exceptions and convert them following the logic Hibernate ORM.
 * <p>
 * Depending on the value of
 * {@link org.hibernate.cfg.AvailableSettings#NATIVE_EXCEPTION_HANDLING_51_COMPLIANCE}
 * it converts the exception to a {@link HibernateException} or {@link javax.persistence.PersistenceException}.
 *
 * @see org.hibernate.engine.spi.ExceptionConverter
 * @see org.hibernate.cfg.AvailableSettings#NATIVE_EXCEPTION_HANDLING_51_COMPLIANCE
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
