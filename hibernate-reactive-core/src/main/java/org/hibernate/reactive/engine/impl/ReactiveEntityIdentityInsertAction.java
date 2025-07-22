/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.action.internal.EntityIdentityInsertAction;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.generator.values.GeneratedValues;
import org.hibernate.internal.util.NullnessUtil;
import org.hibernate.metamodel.mapping.EntityRowIdMapping;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * A reactific {@link EntityIdentityInsertAction} (used when
 * inserting into tables with autoincrement columns).
 */
public class ReactiveEntityIdentityInsertAction extends EntityIdentityInsertAction implements ReactiveEntityInsertAction {

	private final boolean isVersionIncrementDisabled;
	private boolean executed;
	private boolean transientReferencesNullified;
	private Object rowId;

	public ReactiveEntityIdentityInsertAction(
			Object[] state,
			Object instance,
			EntityPersister persister,
			boolean isVersionIncrementDisabled,
			EventSource session,
			boolean isDelayed) {
		super( state, instance, persister, isVersionIncrementDisabled, session, isDelayed );
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

		setVeto( preInsert() );

		// Don't need to lock the cache here, since if someone
		// else inserted the same pk first, the insert would fail

		if ( !isVeto() ) {
			final ReactiveEntityPersister reactivePersister = (ReactiveEntityPersister) persister;
			return stage
					.thenCompose( v -> reactivePersister.insertReactive( getState(), instance, session ) )
					.thenCompose( generatedValues -> {
						Object generatedId = extractGeneratedId( generatedValues );
						setGeneratedId( generatedId );
						return processInsertGeneratedProperties( reactivePersister, generatedId, instance, generatedValues, session )
								.thenAccept( v -> {
									//need to do that here rather than in the save event listener to let
									//the post insert events to have an id-filled entity when IDENTITY is used (EJB3)
									persister.setIdentifier( instance, generatedId, session );
									final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
									persistenceContext.registerInsertedKey( getPersister(), generatedId );
									final EntityKey entityKey = session.generateEntityKey( generatedId, persister );
									setEntityKey( entityKey );
									persistenceContext.checkUniqueness( entityKey, getInstance() );

									postInsert();

									final StatisticsImplementor statistics = session.getFactory().getStatistics();
									if ( statistics.isStatisticsEnabled() && !isVeto() ) {
										statistics.insertEntity( getPersister().getEntityName() );
									}

									markExecuted();
								} );
					} );
		}
		else {
			postInsert();
			markExecuted();
			return stage;
		}
	}

	private Object extractGeneratedId(GeneratedValues generatedValues) {
		return NullnessUtil.castNonNull( generatedValues )
				.getGeneratedValue( getPersister().getIdentifierMapping() );
	}

	private CompletionStage<Void> processInsertGeneratedProperties(
			ReactiveEntityPersister persister,
			Object generatedId,
			Object instance,
			GeneratedValues generatedValues,
			SharedSessionContractImplementor session) {
		final EntityRowIdMapping rowIdMapping = persister.getRowIdMapping();
		if ( rowIdMapping != null ) {
			rowId = generatedValues.getGeneratedValue( rowIdMapping );
			if ( rowId != null && !isEarlyInsert() ) {
				session.getPersistenceContext().replaceEntityEntryRowId( getInstance(), rowId );
			}
		}
		return persister.hasInsertGeneratedProperties()
				? persister.reactiveProcessInsertGenerated( generatedId, instance, getState(), generatedValues, session )
				: voidFuture();
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

	@Override
	public Object getRowId() {
		return rowId;
	}
}
