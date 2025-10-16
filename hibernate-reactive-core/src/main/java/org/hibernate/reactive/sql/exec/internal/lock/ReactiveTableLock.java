/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.sql.exec.internal.lock;

import org.hibernate.AssertionFailure;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metamodel.mapping.AttributeMapping;
import org.hibernate.metamodel.mapping.EntityMappingType;
import org.hibernate.metamodel.mapping.ForeignKeyDescriptor;
import org.hibernate.metamodel.mapping.TableDetails;
import org.hibernate.metamodel.mapping.internal.ToOneAttributeMapping;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.session.impl.ReactiveSessionImpl;
import org.hibernate.reactive.sql.exec.internal.StandardReactiveSelectExecutor;
import org.hibernate.reactive.sql.results.spi.ReactiveListResultsConsumer;
import org.hibernate.sql.ast.tree.select.SelectStatement;
import org.hibernate.sql.exec.internal.BaseExecutionContext;
import org.hibernate.sql.exec.internal.lock.EntityDetails;
import org.hibernate.sql.exec.internal.lock.TableLock;
import org.hibernate.sql.results.graph.DomainResult;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import static org.hibernate.Hibernate.isEmpty;
import static org.hibernate.reactive.util.impl.CompletionStages.nullFuture;

/**
 * Reactive version of {@link TableLock}
 */
public class ReactiveTableLock extends TableLock {
	Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public ReactiveTableLock(
			TableDetails tableDetails,
			EntityMappingType entityMappingType,
			List<EntityKey> entityKeys,
			SharedSessionContractImplementor session) {
		super( tableDetails, entityMappingType, entityKeys, session );
	}

	@Override
	public void applyAttribute(int index, AttributeMapping attributeMapping) {
		final var attributePath = rootPath.append( attributeMapping.getPartName() );
		final DomainResult<Object> domainResult;
		final ResultHandler resultHandler;
		if ( attributeMapping instanceof ToOneAttributeMapping toOne ) {
			domainResult =
					toOne.getForeignKeyDescriptor().getKeyPart()
							.createDomainResult(
									attributePath,
									logicalTableGroup,
									ForeignKeyDescriptor.PART_NAME,
									creationStates
							);
			resultHandler = new ReactiveToOneResultHandler( index, toOne );
		}
		else {
			domainResult =
					attributeMapping.createDomainResult(
							attributePath,
							logicalTableGroup,
							null,
							creationStates
					);
			resultHandler = new NonToOneResultHandler( index );
		}
		domainResults.add( domainResult );
		resultHandlers.add( resultHandler );
	}

	@Override
	public void performActions(
			Map<Object, EntityDetails> entityDetailsMap,
			QueryOptions lockingQueryOptions,
			SharedSessionContractImplementor session) {
		throw LOG.nonReactiveMethodCall( "reactivePerformActions()" );
	}

	public CompletionStage<Void> reactivePerformActions(
			Map<Object, EntityDetails> entityDetailsMap,
			QueryOptions lockingQueryOptions,
			ReactiveSessionImpl session) {
		final var sessionFactory = session.getSessionFactory();
		final var jdbcServices = sessionFactory.getJdbcServices();
		final var selectStatement = new SelectStatement( querySpec, domainResults );

		return StandardReactiveSelectExecutor.INSTANCE
				.list(
						jdbcServices.getDialect().getSqlAstTranslatorFactory()
								.buildSelectTranslator( sessionFactory, selectStatement )
								.translate( jdbcParameterBindings, lockingQueryOptions ),
						jdbcParameterBindings,
						// IMPORTANT: we need a "clean" ExecutionContext to not further apply locking
						new BaseExecutionContext( session ),
						row -> row,
						Object[].class,
						ReactiveListResultsConsumer.UniqueSemantic.ALLOW
				).thenApply( results -> {
					if ( isEmpty( results ) ) {
						throw new AssertionFailure( "Expecting results" );
					}

					results.forEach( (row) -> {
						final Object id = row[0];
						final var entityDetails = entityDetailsMap.get( id );
						for ( int i = 0; i < resultHandlers.size(); i++ ) {
							// offset 1 because of the id at position 0
							resultHandlers.get( i ).applyResult( row[i + 1], entityDetails, session );
						}
					} );
					return null;
				} );
	}

	protected static class ReactiveToOneResultHandler extends ToOneResultHandler {
		Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

		public ReactiveToOneResultHandler(Integer statePosition, ToOneAttributeMapping toOne) {
			super( statePosition, toOne );
		}

		public void applyResult(
				Object stateValue,
				EntityDetails entityDetails,
				SharedSessionContractImplementor session) {
			throw LOG.nonReactiveMethodCall( "reactiveApplyResult()" );
		}

		public CompletionStage<Void> reactiveApplyResult(
				Object stateValue,
				EntityDetails entityDetails,
				ReactiveSessionImpl session) {
			final Object reference;
			if ( stateValue == null ) {
				if ( !toOne.isNullable() ) {
					throw new IllegalStateException( "Retrieved key was null, but to-one is not nullable : " + toOne.getNavigableRole()
							.getFullPath() );
				}
				else {
					reference = null;
				}
				applyLoadedState( entityDetails, statePosition, reference );
				applyModelState( entityDetails, statePosition, reference );
				return nullFuture();
			}
			else {
				return session.reactiveInternalLoad(
						toOne.getAssociatedEntityMappingType().getEntityName(),
						stateValue,
						false,
						toOne.isNullable()
				).thenApply( ref -> {
					applyLoadedState( entityDetails, statePosition, ref );
					applyModelState( entityDetails, statePosition, ref );
					return null;
				} );
			}

		}
	}
}
