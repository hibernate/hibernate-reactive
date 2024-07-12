/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.LockMode;
import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.engine.internal.NonNullableTransientDependencies;
import org.hibernate.engine.internal.Nullability;
import org.hibernate.engine.spi.ComparableExecutable;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityHolder;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.engine.ReactiveActionQueue;
import org.hibernate.reactive.engine.ReactiveExecutable;

import static org.hibernate.engine.internal.Versioning.getVersion;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

/**
 * Abstracts over {@link ReactiveEntityRegularInsertAction} and {@link ReactiveEntityIdentityInsertAction}.
 * Needed in {@link ReactiveActionQueue}.
 *
 * @see org.hibernate.action.internal.AbstractEntityInsertAction
 */
public interface ReactiveEntityInsertAction extends ReactiveExecutable, ComparableExecutable {
	boolean isEarlyInsert();
	NonNullableTransientDependencies findNonNullableTransientEntities();
	SharedSessionContractImplementor getSession();
	boolean isVeto();
	Object getInstance();
	String getEntityName();
	Object[] getState();
	Object getRowId();
	EntityPersister getPersister();

	boolean isExecuted();
	boolean isVersionIncrementDisabled();
	boolean areTransientReferencesNullified();
	void setTransientReferencesNullified();
	EntityKey getEntityKey();

	/**
	 * Nullifies any references to transient entities in the entity state
	 * maintained by this action. References to transient entities
	 * should be nullified when an entity is made "managed" or when this
	 * action is executed, whichever is first.
	 * <p/>
	 * References will only be nullified the first time this method is
	 * called for a this object, so it can safely be called both when
	 * the entity is made "managed" and when this action is executed.
	 *
	 * @see #reactiveMakeEntityManaged()
	 */
	// @see org.hibernate.action.internal.AbstractEntityInsertAction#nullifyTransientReferencesIfNotAlready()
	default CompletionStage<Void> reactiveNullifyTransientReferencesIfNotAlready() {
		if ( !areTransientReferencesNullified() ) {
			return new ForeignKeys.Nullifier( getInstance(), false, isEarlyInsert(), (SessionImplementor) getSession(), getPersister() )
					.nullifyTransientReferences( getState() )
					.thenAccept( v -> {
						new Nullability( getSession() ).checkNullability( getState(), getPersister(), false );
						setTransientReferencesNullified();
					} );
		}
		else {
			return voidFuture();
		}
	}

	/**
	 * Make the entity "managed" by the persistence context.
	 *
	 * @see org.hibernate.action.internal.AbstractEntityInsertAction#makeEntityManaged()
	 */
	default CompletionStage<Void> reactiveMakeEntityManaged() {
		return reactiveNullifyTransientReferencesIfNotAlready()
				.thenAccept( v -> {
					final Object version = getVersion( getState(), getPersister() );
					final PersistenceContext persistenceContextInternal = getSession().getPersistenceContextInternal();
					final EntityHolder entityHolder = persistenceContextInternal
							.addEntityHolder( getEntityKey(), getInstance() );
					final EntityEntry entityEntry = persistenceContextInternal.addEntry(
							getInstance(),
							( getPersister().isMutable() ? Status.MANAGED : Status.READ_ONLY ),
							getState(),
							getRowId(),
							getEntityKey().getIdentifier(),
							version,
							LockMode.WRITE,
							isExecuted(),
							getPersister(),
							isVersionIncrementDisabled()
					);
					entityHolder.setEntityEntry( entityEntry );
					if ( isEarlyInsert() ) {
						addCollectionsByKeyToPersistenceContext( persistenceContextInternal, getState() );
					}
				});
	}

	void addCollectionsByKeyToPersistenceContext(PersistenceContext persistenceContext, Object[] objects);

	default CompletionStage<NonNullableTransientDependencies> reactiveFindNonNullableTransientEntities() {
		return ForeignKeys.findNonNullableTransientEntities( getPersister().getEntityName(), getInstance(), getState(), isEarlyInsert(), getSession() );
	}

	AbstractEntityInsertAction asAbstractEntityInsertAction();
}
