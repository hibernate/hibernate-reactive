/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.query.sql.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.results.ResultSetMapping;
import org.hibernate.query.spi.DomainQueryExecutionContext;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.query.sql.internal.NativeSelectQueryPlanImpl;
import org.hibernate.query.sql.internal.ResultSetMappingProcessor;
import org.hibernate.query.sql.internal.SQLQueryParser;
import org.hibernate.query.sql.spi.ParameterOccurrence;
import org.hibernate.query.sqm.internal.SqmJdbcExecutionContextAdapter;
import org.hibernate.reactive.engine.spi.ReactiveSharedSessionContractImplementor;
import org.hibernate.reactive.query.internal.ReactiveResultSetMappingProcessor;
import org.hibernate.reactive.query.spi.ReactiveNativeSelectQueryPlan;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;
import org.hibernate.sql.results.jdbc.spi.JdbcValuesMappingProducer;

import static java.util.Collections.emptyList;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class ReactiveNativeSelectQueryPlanImpl<R> extends NativeSelectQueryPlanImpl<R> implements ReactiveNativeSelectQueryPlan<R> {

	private final Set<String> affectedTableNames;
	private final String sql;
	private final List<ParameterOccurrence> parameterList;
	private final JdbcValuesMappingProducer resultSetMapping;

	public ReactiveNativeSelectQueryPlanImpl(
			String sql,
			Set<String> affectedTableNames,
			List<ParameterOccurrence> parameterList,
			ResultSetMapping resultSetMapping,
			SessionFactoryImplementor sessionFactory) {
		super( sql, affectedTableNames, parameterList, resultSetMapping, sessionFactory );
		final ResultSetMappingProcessor processor = new ReactiveResultSetMappingProcessor( resultSetMapping, sessionFactory );
		final SQLQueryParser parser = new SQLQueryParser( sql, processor.process(), sessionFactory );
		this.resultSetMapping = processor.generateResultMapping( parser.queryHasAliases() );
		if ( affectedTableNames == null ) {
			affectedTableNames = new HashSet<>();
		}
		if ( resultSetMapping != null ) {
			resultSetMapping.addAffectedTableNames( affectedTableNames, sessionFactory );
		}
		this.affectedTableNames = affectedTableNames;
		this.sql = parser.process();
		this.parameterList = parameterList;

	}

	@Override
	public CompletionStage<List<R>> reactivePerformList(DomainQueryExecutionContext executionContext) {
		final QueryOptions queryOptions = executionContext.getQueryOptions();
		if ( queryOptions.getEffectiveLimit().getMaxRowsJpa() == 0 ) {
			return completedFuture( emptyList() );
		}

		final List<JdbcParameterBinder> jdbcParameterBinders;
		final JdbcParameterBindings jdbcParameterBindings;

		final QueryParameterBindings queryParameterBindings = executionContext.getQueryParameterBindings();
		if ( parameterList == null || parameterList.isEmpty() ) {
			jdbcParameterBinders = emptyList();
			jdbcParameterBindings = JdbcParameterBindings.NO_BINDINGS;
		}
		else {
			jdbcParameterBinders = new ArrayList<>( parameterList.size() );
			jdbcParameterBindings = new JdbcParameterBindingsImpl(
					queryParameterBindings,
					parameterList,
					jdbcParameterBinders,
					executionContext.getSession().getFactory()
			);
		}

		return ( (ReactiveSharedSessionContractImplementor) executionContext.getSession() )
				.reactiveAutoFlushIfRequired( affectedTableNames )
				.thenCompose( aBoolean -> {
					final JdbcOperationQuerySelect jdbcSelect = new JdbcOperationQuerySelect(
							sql,
							jdbcParameterBinders,
							resultSetMapping,
							affectedTableNames
					);

					return StandardReactiveSelectExecutor.INSTANCE
							.list(
									jdbcSelect,
									jdbcParameterBindings,
									SqmJdbcExecutionContextAdapter.usingLockingAndPaging( executionContext ),
									null,
									queryOptions.getUniqueSemantic() == null
											? ReactiveListResultsConsumer.UniqueSemantic.NEVER
											: reactiveUniqueSemantic( queryOptions )
							);

				} );
	}

	private static ReactiveListResultsConsumer.UniqueSemantic reactiveUniqueSemantic(QueryOptions queryOptions) {
		switch ( queryOptions.getUniqueSemantic() ) {
			case NONE:
				return ReactiveListResultsConsumer.UniqueSemantic.NONE;
			case FILTER:
				return ReactiveListResultsConsumer.UniqueSemantic.FILTER;
			case ASSERT:
				return ReactiveListResultsConsumer.UniqueSemantic.ASSERT;
			case NEVER:
				return ReactiveListResultsConsumer.UniqueSemantic.NEVER;
			case ALLOW:
				return ReactiveListResultsConsumer.UniqueSemantic.ALLOW;
			default:
				throw new IllegalArgumentException( "Unique semantic option not recognized: " + queryOptions.getUniqueSemantic() );
		}
	}
}
