/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import org.hibernate.HibernateException;
import org.hibernate.action.internal.EntityIdentityInsertAction;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.persister.entity.impl.ReactiveEntityPersister;
import org.hibernate.stat.spi.StatisticsImplementor;

import java.util.concurrent.CompletionStage;

/**
 * A reactific {@link EntityIdentityInsertAction} (used when
 * inserting into tables with autoincrement columns).
 */
public class ReactiveEntityIdentityInsertAction extends EntityIdentityInsertAction implements ReactiveEntityInsertAction {

	private final boolean isVersionIncrementDisabled;
	private boolean executed;
	private boolean transientReferencesNullified;

	public ReactiveEntityIdentityInsertAction(
			Object[] state,
			Object instance,
			EntityPersister persister,
			boolean isVersionIncrementDisabled,
			SharedSessionContractImplementor session,
			boolean isDelayed) {
		super(state, instance, persister, isVersionIncrementDisabled, session, isDelayed);
		this.isVersionIncrementDisabled = isVersionIncrementDisabled;
	}

	@Override
	public void execute() throws HibernateException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletionStage<Void> reactiveExecute() throws HibernateException {

		CompletionStage<Void> stage = reactiveNullifyTransientReferencesIfNotAlready();

		final EntityPersister persister = getPersister();
		final SharedSessionContractImplementor session = getSession();
		final Object instance = getInstance();

		setVeto( preInsert() );

		// Don't need to lock the cache here, since if someone
		// else inserted the same pk first, the insert would fail

		if ( !isVeto() ) {
			return stage.thenCompose( v -> ( (ReactiveEntityPersister) persister ).insertReactive( getState(), instance, session ) )
					.thenAccept( generatedId -> {
						setGeneratedId(generatedId);
						if (persister.hasInsertGeneratedProperties()) {
							persister.processInsertGeneratedProperties(generatedId, instance, getState(), session);
						}
						//need to do that here rather than in the save event listener to let
						//the post insert events to have a id-filled entity when IDENTITY is used (EJB3)
						persister.setIdentifier(instance, generatedId, session);
						final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
						persistenceContext.registerInsertedKey(getPersister(), generatedId);
						EntityKey entityKey = session.generateEntityKey(generatedId, persister);
						setEntityKey( entityKey );
						persistenceContext.checkUniqueness(entityKey, getInstance());

						postInsert();

						final StatisticsImplementor statistics = session.getFactory().getStatistics();
						if ( statistics.isStatisticsEnabled() && !isVeto() ) {
							statistics.insertEntity( getPersister().getEntityName() );
						}

						markExecuted();
					});
			}
			else {
				postInsert();
				markExecuted();
				return stage;
			}
	}

	@Override
	public EntityKey getEntityKey() {
		return super.getEntityKey();
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
}
