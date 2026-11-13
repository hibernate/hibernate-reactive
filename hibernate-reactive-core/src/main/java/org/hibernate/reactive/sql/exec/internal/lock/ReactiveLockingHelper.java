/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.engine.spi.CollectionKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.PluralAttributeMapping;
import org.hibernate.query.internal.QueryOptionsImpl;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.dialect.sql.ast.spi.SqlAstTranslationRequest;
import org.hibernate.sql.ast.spi.query.expression.ColumnReference;
import org.hibernate.sql.ast.spi.query.expression.JdbcParameter;
import org.hibernate.sql.ast.spi.query.expression.SqlTuple;
import org.hibernate.sql.ast.spi.query.from.NamedTableReference;
import org.hibernate.sql.ast.spi.query.from.TableReference;
import org.hibernate.sql.ast.spi.query.predicate.InListPredicate;
import org.hibernate.sql.ast.spi.query.select.QuerySpec;
import org.hibernate.sql.ast.spi.query.select.SelectStatement;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.exec.internal.JdbcParameterBindingImpl;
import org.hibernate.sql.exec.internal.JdbcParameterBindingsImpl;
import org.hibernate.sql.exec.internal.SqlTypedMappingJdbcParameter;
import org.hibernate.sql.exec.internal.lock.EntityDetails;
import org.hibernate.sql.exec.internal.lock.LockingHelper;
import org.hibernate.sql.exec.internal.lock.LockingTableGroup;
import org.hibernate.sql.exec.internal.lock.SqlSelectionImpl;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBindings;

import jakarta.persistence.Timeout;

import static org.hibernate.sql.exec.SqlExecLogger.SQL_EXEC_LOGGER;

/**
 * Reactive version of the methods of {@link org.hibernate.sql.exec.internal.lock.LockingHelper}
 * that lock the rows of the collection tables, which execute blocking JDBC statements.
 */
public final class ReactiveLockingHelper {

	private ReactiveLockingHelper() {
	}

	/**
	 * Lock the rows of the collection-table owned by the entities in {@code ownerDetailsMap}.
	 *
	 * @see org.hibernate.sql.exec.internal.lock.LockingHelper#lockCollectionTable(PluralAttributeMapping, LockMode, Timeout, Map, ExecutionContext)
	 */
	public static CompletionStage<Void> lockCollectionTable(
			PluralAttributeMapping attributeMapping,
			LockMode lockMode,
			Timeout lockTimeout,
			Map<Object, EntityDetails> ownerDetailsMap,
			ExecutionContext executionContext) {
		final var session = executionContext.getSession();
		final List<Object> keys = new ArrayList<>( ownerDetailsMap.size() );
		ownerDetailsMap.forEach( (o, entityDetails) ->
				keys.add( LockingHelper.resolveCollectionKey( attributeMapping, entityDetails.instance(), session ) ) );
		return lockCollectionTable( attributeMapping, lockMode, lockTimeout, keys, session );
	}

	/**
	 * Lock the rows of the collection-table for the given {@code collectionKeys}.
	 *
	 * @see org.hibernate.sql.exec.internal.lock.LockingHelper#lockCollectionTable(PluralAttributeMapping, LockMode, Timeout, List, ExecutionContext)
	 */
	public static CompletionStage<Void> lockCollectionTable(
			PluralAttributeMapping attributeMapping,
			LockMode lockMode,
			Timeout lockTimeout,
			List<CollectionKey> collectionKeys,
			ExecutionContext executionContext) {
		final List<Object> keys = new ArrayList<>( collectionKeys.size() );
		for ( CollectionKey collectionKey : collectionKeys ) {
			keys.add( collectionKey.getKey() );
		}
		return lockCollectionTable( attributeMapping, lockMode, lockTimeout, keys, executionContext.getSession() );
	}

	private static CompletionStage<Void> lockCollectionTable(
			PluralAttributeMapping attributeMapping,
			LockMode lockMode,
			Timeout lockTimeout,
			List<Object> keys,
			SharedSessionContractImplementor session) {
		final var keyDescriptor = attributeMapping.getKeyDescriptor();
		final String keyTableName = keyDescriptor.getKeyTable();
		if ( SQL_EXEC_LOGGER.isDebugEnabled() ) {
			SQL_EXEC_LOGGER.followOnLockingForCollectionTable( keyTableName, attributeMapping.getRootPathName() );
		}

		final var tableReference = new NamedTableReference( keyTableName, "tbl" );
		final var tableGroup = new LockingTableGroup(
				tableReference,
				keyTableName,
				attributeMapping,
				keyDescriptor.getKeySide().getModelPart()
		);
		final var querySpec = new QuerySpec( true );
		querySpec.getFromClause().addRoot( tableGroup );
		querySpec.applyRootPathForLocking( tableGroup.getNavigablePath() );

		// NOTE: We add the key column to the selection list, but never create a DomainResult
		// as we won't read the value back (same as in ORM)
		final var columnReference = new ColumnReference( tableReference, keyDescriptor.getKeyPart().getSelectable( 0 ) );
		querySpec.getSelectClause().addSqlSelection( new SqlSelectionImpl( columnReference, 0 ) );

		final var parameterBindings = new JdbcParameterBindingsImpl( keys.size() * keyDescriptor.getJdbcTypeCount() );
		querySpec.applyPredicate( keysPredicate( keyDescriptor, tableReference, columnReference, keys, parameterBindings, session ) );

		return performLocking( querySpec, parameterBindings, lockMode, lockTimeout, session );
	}

	private static InListPredicate keysPredicate(
			ForeignKeyDescriptor keyDescriptor,
			TableReference tableReference,
			ColumnReference columnReference,
			List<Object> keys,
			JdbcParameterBindingsImpl parameterBindings,
			SharedSessionContractImplementor session) {
		final int jdbcTypeCount = keyDescriptor.getJdbcTypeCount();
		if ( jdbcTypeCount == 1 ) {
			final var restriction = new InListPredicate( columnReference );
			for ( Object key : keys ) {
				keyDescriptor.breakDownJdbcValues(
						key,
						(valueIndex, value, jdbcValueMapping) -> {
							final var jdbcParameter = new SqlTypedMappingJdbcParameter( jdbcValueMapping );
							restriction.addExpression( jdbcParameter );
							parameterBindings.addBinding(
									jdbcParameter,
									new JdbcParameterBindingImpl( jdbcValueMapping.getJdbcMapping(), value )
							);
						},
						session
				);
			}
			return restriction;
		}

		final List<ColumnReference> columnReferences = new ArrayList<>( jdbcTypeCount );
		keyDescriptor.forEachSelectable( (selectionIndex, selectableMapping) ->
				columnReferences.add( new ColumnReference( tableReference, selectableMapping ) ) );
		final var inListPredicate = new InListPredicate( new SqlTuple( columnReferences, keyDescriptor ) );
		for ( Object key : keys ) {
			final List<JdbcParameter> jdbcParameters = new ArrayList<>( jdbcTypeCount );
			keyDescriptor.breakDownJdbcValues(
					key,
					(valueIndex, value, jdbcValueMapping) -> {
						final var jdbcParameter = new SqlTypedMappingJdbcParameter( jdbcValueMapping );
						jdbcParameters.add( jdbcParameter );
						parameterBindings.addBinding(
								jdbcParameter,
								new JdbcParameterBindingImpl( jdbcValueMapping.getJdbcMapping(), value )
						);
					},
					session
			);
			inListPredicate.addExpression( new SqlTuple( jdbcParameters, keyDescriptor ) );
		}
		return inListPredicate;
	}

	private static CompletionStage<Void> performLocking(
			QuerySpec querySpec,
			JdbcParameterBindings jdbcParameterBindings,
			LockMode lockMode,
			Timeout lockTimeout,
			SharedSessionContractImplementor session) {
		final var lockingQueryOptions = new QueryOptionsImpl();
		final var lockOptions = lockingQueryOptions.getLockOptions();
		lockOptions.setLockMode( lockMode );
		lockOptions.setTimeout( lockTimeout );

		final var sessionFactory = session.getSessionFactory();
		final var jdbcSelect = sessionFactory.getJdbcServices().getDialect().getSqlAstTranslatorFactory()
				.buildTranslator( new SqlAstTranslationRequest.Select( sessionFactory, new SelectStatement( querySpec ) ) )
				.translate( jdbcParameterBindings, lockingQueryOptions );
		return StandardReactiveSelectExecutor.INSTANCE
				.list(
						jdbcSelect,
						jdbcParameterBindings,
						// IMPORTANT: we need a "clean" ExecutionContext to not further apply locking
						new BaseExecutionContext( session ),
						row -> row,
						Object[].class,
						ReactiveListResultsConsumer.UniqueSemantic.ALLOW
				)
				.thenAccept( rows -> {} );
	}
}
