/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionDataAccess;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.SubselectFetch;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.mapping.Collection;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinator;
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinatorNoOp;
import org.hibernate.persister.collection.mutation.DeleteRowsCoordinatorStandard;
import org.hibernate.persister.collection.mutation.InsertRowsCoordinator;
import org.hibernate.persister.collection.mutation.RemoveCoordinator;
import org.hibernate.persister.collection.mutation.RemoveCoordinatorNoOp;
import org.hibernate.persister.collection.mutation.RemoveCoordinatorStandard;
import org.hibernate.persister.collection.mutation.UpdateRowsCoordinator;
import org.hibernate.persister.collection.mutation.UpdateRowsCoordinatorNoOp;
import org.hibernate.persister.collection.mutation.UpdateRowsCoordinatorOneToMany;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoader;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoaderSubSelectFetch;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinatorNoOp;
import org.hibernate.reactive.persister.collection.mutation.ReactiveInsertRowsCoordinatorStandard;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.util.impl.CompletionStages;

import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER;
import static org.hibernate.sql.model.ModelMutationLogging.MODEL_MUTATION_LOGGER_DEBUG_ENABLED;

/**
 * A reactive {@link OneToManyPersister}
 */
public class ReactiveOneToManyPersister extends OneToManyPersister
		implements ReactiveAbstractCollectionPersister {

	private final InsertRowsCoordinator insertRowsCoordinator;
	private final UpdateRowsCoordinator updateRowsCoordinator;
	private final DeleteRowsCoordinator deleteRowsCoordinator;
	private final RemoveCoordinator removeCoordinator;

	public ReactiveOneToManyPersister(
			Collection collectionBinding,
			CollectionDataAccess cacheAccessStrategy,
			RuntimeModelCreationContext creationContext) throws MappingException, CacheException {
		super( collectionBinding, cacheAccessStrategy, creationContext );

		this.insertRowsCoordinator = buildInsertCoordinator();
		this.updateRowsCoordinator = buildUpdateCoordinator();
		this.deleteRowsCoordinator = buildDeleteCoordinator();
		this.removeCoordinator = buildDeleteAllCoordinator();
	}

	private InsertRowsCoordinator buildInsertCoordinator() {
		if ( isInverse() || !isRowInsertEnabled() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection (re)creation - %s", getRolePath() );
			}
			return new ReactiveInsertRowsCoordinatorNoOp( this );
		}
		return new ReactiveInsertRowsCoordinatorStandard( this, getRowMutationOperations() );
	}

	private UpdateRowsCoordinator buildUpdateCoordinator() {
		if ( !isRowDeleteEnabled() && !isRowInsertEnabled() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection row updates - %s", getRolePath() );
			}
			return new UpdateRowsCoordinatorNoOp( this );
		}
		return new UpdateRowsCoordinatorOneToMany( this, getRowMutationOperations(), getFactory() );
	}

	private DeleteRowsCoordinator buildDeleteCoordinator() {
		if ( !needsRemove() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection row deletions - %s", getRolePath() );
			}
			return new DeleteRowsCoordinatorNoOp( this );
		}
		// never delete by index for one-to-many
		return new DeleteRowsCoordinatorStandard( this, getRowMutationOperations(), false );
	}

	private RemoveCoordinator buildDeleteAllCoordinator() {
		if ( ! needsRemove() ) {
			if ( MODEL_MUTATION_LOGGER_DEBUG_ENABLED ) {
				MODEL_MUTATION_LOGGER.debugf( "Skipping collection removals - %s", getRolePath() );
			}
			return new RemoveCoordinatorNoOp( this );
		}
		return new RemoveCoordinatorStandard( this, this::buildDeleteAllOperation );
	}

	private Parameters parameters() {
		return Parameters.instance( getFactory().getJdbcServices().getDialect() );
	}

	@Override
	protected CollectionLoader createCollectionLoader(LoadQueryInfluencers loadQueryInfluencers) {
		return createReactiveCollectionLoader( loadQueryInfluencers );
	}

	@Override
	protected CollectionLoader createSubSelectLoader(SubselectFetch subselect, SharedSessionContractImplementor session) {
		return new ReactiveCollectionLoaderSubSelectFetch( getAttributeMapping(), null, subselect, session );
	}

	@Override
	public CompletionStage<Void> reactiveInitialize(Object key, SharedSessionContractImplementor session) {
		return ( (ReactiveCollectionLoader) determineLoaderToUse( key, session ) )
				.reactiveLoad( key, session )
				.thenCompose( CompletionStages::voidFuture );
	}

	@Override
	public boolean isRowDeleteEnabled() {
		return super.isRowDeleteEnabled();
	}

	@Override
	public boolean isRowInsertEnabled() {
		return super.isRowInsertEnabled();
	}

	@Override
	public boolean hasIdentifier() {
		return super.hasIdentifier;
	}

	@Override
	public boolean indexContainsFormula() {
		return super.indexContainsFormula;
	}

	@Override
	public CompletionStage<Void> recreateReactive(PersistentCollection collection, Object id, SharedSessionContractImplementor session) throws HibernateException {
		throw new NotYetImplementedException();
	}

	@Override
	public CompletionStage<Void> reactiveInsertRows(PersistentCollection collection, Object id, SharedSessionContractImplementor session) throws HibernateException {
		throw new NotYetImplementedException();
	}
}
