/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.results.ResultSetMapping;
import org.hibernate.query.results.ResultSetMappingImpl;
import org.hibernate.reactive.sql.results.ReactiveResultSetMapping;
import org.hibernate.reactive.sql.results.internal.ReactiveStandardValuesMappingProducer;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducerProvider;

public class ReactiveValuesMappingProducerProvider implements JdbcValuesMappingProducerProvider {
	/**
	 * Singleton access
	 */
	public static final ReactiveValuesMappingProducerProvider INSTANCE = new ReactiveValuesMappingProducerProvider();

	@Override
	public JdbcValuesMappingProducer buildMappingProducer(
			SelectStatement sqlAst,
			SessionFactoryImplementor sessionFactory) {
		return new ReactiveStandardValuesMappingProducer(
				sqlAst.getQuerySpec().getSelectClause().getSqlSelections(),
				sqlAst.getDomainResultDescriptors()
		);
	}

	@Override
	public ResultSetMapping buildResultSetMapping(
			String name,
			boolean isDynamic,
			SessionFactoryImplementor sessionFactory) {
		return new ReactiveResultSetMapping( new ResultSetMappingImpl( name, isDynamic ) );
	}
}
