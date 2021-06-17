/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;

import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.spi.FilterTranslator;
import org.hibernate.hql.spi.QueryTranslator;
import org.hibernate.hql.spi.QueryTranslatorFactory;
import org.hibernate.reactive.session.impl.ReactiveQueryTranslatorImpl;

/**
 * Facade for the generation of reactive {@link QueryTranslator} and {@link FilterTranslator} instances.
 *
 * @see ReactiveQueryTranslatorImpl
 */
public class ReactiveQueryTranslatorFactory implements QueryTranslatorFactory {

	@Override
	public QueryTranslator createQueryTranslator(
			String queryIdentifier,
			String queryString,
			Map filters,
			SessionFactoryImplementor factory,
			EntityGraphQueryHint entityGraphQueryHint) {
		return new ReactiveQueryTranslatorImpl<>( queryIdentifier, queryString, filters, factory, entityGraphQueryHint );
	}

	@Override
	public FilterTranslator createFilterTranslator(
			String queryIdentifier, String queryString, Map filters, SessionFactoryImplementor factory) {
		return new ReactiveQueryTranslatorImpl<>( queryIdentifier, queryString, filters, factory  );
	}
}
