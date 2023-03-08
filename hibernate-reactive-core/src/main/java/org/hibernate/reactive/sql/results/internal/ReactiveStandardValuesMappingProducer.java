/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.results.internal;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.sql.results.spi.ReactiveValuesMappingProducer;
import org.hibernate.sql.ast.spi.SqlSelection;
import org.hibernate.sql.results.graph.DomainResult;
import org.hibernate.sql.results.jdbc.internal.JdbcValuesMappingProducerStandard;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMapping;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;


public class ReactiveStandardValuesMappingProducer extends JdbcValuesMappingProducerStandard implements JdbcValuesMappingProducer, ReactiveValuesMappingProducer {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveStandardValuesMappingProducer(List<SqlSelection> sqlSelections, List<DomainResult<?>> domainResults) {
		super( sqlSelections, domainResults );
	}

	@Override
	public JdbcValuesMapping resolve(
			JdbcValuesMetadata jdbcResultsMetadata,
			LoadQueryInfluencers loadQueryInfluencers,
			SessionFactoryImplementor sessionFactory) {
		// In this class the ValuesMapping has been already resolved, so it should be fine to implement this method
		return super.resolve( jdbcResultsMetadata, loadQueryInfluencers, sessionFactory );
	}

	@Override
		public CompletionStage<JdbcValuesMapping> reactiveResolve(
				JdbcValuesMetadata jdbcResultsMetadata,
				LoadQueryInfluencers loadQueryInfluencers,
				SessionFactoryImplementor sessionFactory) {
		return completedFuture( super.resolve( jdbcResultsMetadata, loadQueryInfluencers, sessionFactory ) );
	}
}
