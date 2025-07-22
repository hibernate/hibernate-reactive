/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.action.internal.EntityInsertAction;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.metamodel.mapping.EntityRowIdMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactive {@link EntityInsertAction}.
 */
public class ReactiveEntityRegularInsertAction extends EntityInsertAction implements ReactiveEntityInsertAction {

	private final boolean isVersionIncrementDisabled;
	private boolean executed;
	private boolean transientReferencesNullified;

	public ReactiveEntityRegularInsertAction(
			Object id,
			Object[] state,
			Object instance,
			Object version,
			EntityPersister persister,
			boolean isVersionIncrementDisabled,
			EventSource session) {
		super( id, state, instance, version, persister, isVersionIncrementDisabled, session );
		this.isVersionIncrementDisabled = isVersionIncrementDisabled;
	}

	@Override
	public void execute() throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {
		final CompletionStage<Void> stage = reactiveNullifyTransientReferencesIfNotAlready();
		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();
		final Object id = getId();
		final boolean veto = preInsert();
		// Don't need to lock the cache here, since if someone
		// else inserted the same pk first, the insert would fail
		if ( !veto ) {
			final ReactiveEntityPersister reactivePersister = (ReactiveEntityPersister) persister;
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			return stage
					.thenCompose( v -> reactivePersister.insertReactive( id, getState(), instance, session ) )
					.thenCompose( generatedValues -> {
						final EntityEntry entry = persistenceContext.getEntry( instance );
						if ( entry == null ) {
							throw new AssertionFailure( "possible non-threadsafe access to session" );
						}
						entry.postInsert( getState() );
						return processInsertGeneratedProperties( reactivePersister, session, instance, id, generatedValues, entry )
								.thenAccept( vv -> {
									persistenceContext.registerInsertedKey( persister, getId() );
									addCollectionsByKeyToPersistenceContext( persistenceContext, getState() );
									putCacheIfNecessary();
									handleNaturalIdPostSaveNotifications( id );
									postInsert();
									final StatisticsImplementor statistics = session.getFactory().getStatistics();
									if ( statistics.isStatisticsEnabled() ) {
										statistics.insertEntity( getPersister().getEntityName() );
									}
									markExecuted();
								} );
					} );
		}
		else {
			putCacheIfNecessary();
			handleNaturalIdPostSaveNotifications( id );
			postInsert();
			final StatisticsImplementor statistics = session.getFactory().getStatistics();
			if ( statistics.isStatisticsEnabled() ) {
				statistics.insertEntity( getPersister().getEntityName() );
			}
			markExecuted();
			return stage;
		}
	}

	private CompletionStage<Void> processInsertGeneratedProperties(
			ReactiveEntityPersister persister,
			SharedSessionContractImplementor session,
			Object instance,
			Object id,
			GeneratedValues generatedValues,
			EntityEntry entry) {
		if ( persister.hasInsertGeneratedProperties() ) {
			if ( persister.isVersionPropertyGenerated() ) {
				throw new UnsupportedOperationException( "generated version attribute not supported in Hibernate Reactive" );
//				setVersion( Versioning.getVersion( getState(), persister ) );
			}
			return persister.reactiveProcessInsertGenerated( id, instance, getState(), generatedValues, session )
					.thenAccept( v -> {
						// Process row-id values when available early by replacing the entity entry
						if ( generatedValues != null ) {
							final EntityRowIdMapping rowIdMapping = persister.getRowIdMapping();
							if ( rowIdMapping != null ) {
								final Object rowId = generatedValues.getGeneratedValue( rowIdMapping );
								if ( rowId != null ) {
									session.getPersistenceContext().replaceEntityEntryRowId( getInstance(), rowId );
								}
							}
						}
						entry.postUpdate( instance, getState(), getVersion() );
					} );
		}
		else {
			return voidFuture();
		}
	}

	@Override
	public EntityKey getEntityKey() {
		return super.getEntityKey();
	}

	@Override
	public void addCollectionsByKeyToPersistenceContext(PersistenceContext persistenceContext, Object[] objects) {
		super.addCollectionsByKeyToPersistenceContext( persistenceContext, objects );
	}

	@Override
	public AbstractEntityInsertAction asAbstractEntityInsertAction() {
		return this;
	}

	@Override
	protected void markExecuted() {
		super.markExecuted();
		executed = true;
	}

	@Override
	public Object getRowId() {
		return super.getRowId();
	}

	@Override
	public boolean isExecuted() {
		return executed;
	}

	@Override
	public boolean isVersionIncrementDisabled() {
		return isVersionIncrementDisabled;
	}

	@Override
	public boolean areTransientReferencesNullified() {
		return transientReferencesNullified;
	}

	@Override
	public void setTransientReferencesNullified() {
		transientReferencesNullified = true;
	}
}
