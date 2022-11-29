/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader.ast.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockOptions;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.internal.LoaderSelectBuilder;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ModelPart;
import org.hibernate.metamodel.mapping.SingularAttributeMapping;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryOptionsAdapter;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.reactive.loader.ast.spi.ReactiveSingleUniqueKeyEntityLoader;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.SqlAstTranslatorFactory;
import org.hibernate.sql.ast.tree.expression.JdbcParameter;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.CallbackImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.spi.Callback;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcOperationQuerySelect;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

/**
 *
 * @param <T>
 * @see org.hibernate.loader.ast.internal.SingleUniqueKeyEntityLoaderStandard
 */
public class ReactiveSingleUniqueKeyEntityLoaderStandard<T> implements ReactiveSingleUniqueKeyEntityLoader<T> {
	private final EntityMappingType entityDescriptor;
	private final ModelPart uniqueKeyAttribute;

	public ReactiveSingleUniqueKeyEntityLoaderStandard(
			EntityMappingType entityDescriptor,
			SingularAttributeMapping uniqueKeyAttribute) {
		this.entityDescriptor = entityDescriptor;
		if ( uniqueKeyAttribute instanceof ToOneAttributeMapping ) {
			this.uniqueKeyAttribute = ( (ToOneAttributeMapping) uniqueKeyAttribute ).getForeignKeyDescriptor();
		}
		else {
			this.uniqueKeyAttribute = uniqueKeyAttribute;
		}
	}

	@Override
	public EntityMappingType getLoadable() {
		return entityDescriptor;
	}

	@Override
	public CompletionStage<T> load(Object ukValue, LockOptions lockOptions, Boolean readOnly, SharedSessionContractImplementor session) {
		final SessionFactoryImplementor sessionFactory = session.getFactory();

		// todo (6.0) : cache the SQL AST and JdbcParameters
		final List<JdbcParameter> jdbcParameters = new ArrayList<>();
		final SelectStatement sqlAst = LoaderSelectBuilder.createSelectByUniqueKey(
				entityDescriptor,
				Collections.emptyList(),
				uniqueKeyAttribute,
				null,
				1,
				LoadQueryInfluencers.NONE,
				LockOptions.NONE,
				jdbcParameters::add,
				sessionFactory
		);

		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();

		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( jdbcParameters.size() );
		int offset = jdbcParameterBindings.registerParametersForEachJdbcValue(
				ukValue,
				Clause.WHERE,
				uniqueKeyAttribute,
				jdbcParameters,
				session
		);
		assert offset == jdbcParameters.size();
		final JdbcOperationQuerySelect jdbcSelect = sqlAstTranslatorFactory
				.buildSelectTranslator( sessionFactory, sqlAst )
				.translate( jdbcParameterBindings, QueryOptions.NONE );

		return StandardReactiveSelectExecutor.INSTANCE
				.list(
						jdbcSelect,
						jdbcParameterBindings,
						listExecutionContext( readOnly, session ),
						row -> row[0],
						ReactiveListResultsConsumer.UniqueSemantic.FILTER
				)
				.thenApply( list -> singleResult( ukValue, list ) );
	}

	private T singleResult(Object ukValue, List<Object> list) {
		switch ( list.size() ) {
			case 0:
				return null;
			case 1:
				return (T) list.get( 0 );
			default:
				throw new HibernateException( "More than one row with the given identifier was found: " + ukValue + ", for class: " + entityDescriptor.getEntityName() );
		}
	}

	private static ExecutionContext listExecutionContext(Boolean readOnly, SharedSessionContractImplementor session) {
		return new ExecutionContext() {
			private final Callback callback = new CallbackImpl();

			@Override
			public SharedSessionContractImplementor getSession() {
				return session;
			}

			@Override
			public QueryOptions getQueryOptions() {
				return new QueryOptionsAdapter() {
					@Override
					public Boolean isReadOnly() {
						return readOnly;
					}
				};
			}

			@Override
			public String getQueryIdentifier(String sql) {
				return sql;
			}

			@Override
			public QueryParameterBindings getQueryParameterBindings() {
				return QueryParameterBindings.NO_PARAM_BINDINGS;
			}

			@Override
			public Callback getCallback() {
				return callback;
			}

		};
	}

	@Override
	public Object resolveId(Object ukValue, SharedSessionContractImplementor session) {
		final SessionFactoryImplementor sessionFactory = session.getFactory();

		// todo (6.0) : cache the SQL AST and JdbcParameters
		final List<JdbcParameter> jdbcParameters = new ArrayList<>();
		final SelectStatement sqlAst = LoaderSelectBuilder.createSelectByUniqueKey(
				entityDescriptor,
				Collections.singletonList( entityDescriptor.getIdentifierMapping() ),
				uniqueKeyAttribute,
				null,
				1,
				LoadQueryInfluencers.NONE,
				LockOptions.NONE,
				jdbcParameters::add,
				sessionFactory
		);

		final JdbcServices jdbcServices = sessionFactory.getJdbcServices();
		final JdbcEnvironment jdbcEnvironment = jdbcServices.getJdbcEnvironment();
		final SqlAstTranslatorFactory sqlAstTranslatorFactory = jdbcEnvironment.getSqlAstTranslatorFactory();

		final JdbcParameterBindings jdbcParameterBindings = new JdbcParameterBindingsImpl( jdbcParameters.size() );
		int offset = jdbcParameterBindings.registerParametersForEachJdbcValue(
				ukValue,
				Clause.WHERE,
				uniqueKeyAttribute,
				jdbcParameters,
				session
		);
		assert offset == jdbcParameters.size();
		final JdbcOperationQuerySelect jdbcSelect = sqlAstTranslatorFactory.buildSelectTranslator( sessionFactory, sqlAst )
				.translate( jdbcParameterBindings, QueryOptions.NONE );

		return StandardReactiveSelectExecutor.INSTANCE
				.list(
						jdbcSelect,
						jdbcParameterBindings,
						resolveIdExecutionContext( session ),
						row -> row[0],
						ReactiveListResultsConsumer.UniqueSemantic.FILTER
				)
				.thenApply( list -> {
					// FIXME: This is what ORM does, but should this be an Hibernate exception?
					assert list.size() == 1;
					return list.get( 0 );
				} );
	}

	private static ExecutionContext resolveIdExecutionContext(SharedSessionContractImplementor session) {
		return new ExecutionContext() {
			@Override
			public SharedSessionContractImplementor getSession() {
				return session;
			}

			@Override
			public QueryOptions getQueryOptions() {
				return QueryOptions.NONE;
			}

			@Override
			public String getQueryIdentifier(String sql) {
				return sql;
			}

			@Override
			public QueryParameterBindings getQueryParameterBindings() {
				return QueryParameterBindings.NO_PARAM_BINDINGS;
			}

			@Override
			public Callback getCallback() {
				throw new UnsupportedOperationException( "Follow-on locking not supported yet" );
			}

		};
	}
}
