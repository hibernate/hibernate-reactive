package org.hibernate.rx.engine.impl;

import org.hibernate.LockMode;
import org.hibernate.engine.internal.NonNullableTransientDependencies;
import org.hibernate.engine.internal.Nullability;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.rx.engine.spi.RxExecutable;
import org.hibernate.rx.util.impl.RxUtil;

import java.util.concurrent.CompletionStage;

/**
 * Abstracts over {@link RxEntityRegularInsertAction} and {@link RxEntityIdentityInsertAction}.
 * Needed in {@link org.hibernate.rx.engine.spi.RxActionQueue}.
 */
public interface RxEntityInsertAction extends RxExecutable {
	boolean isEarlyInsert();
	NonNullableTransientDependencies findNonNullableTransientEntities();
	SharedSessionContractImplementor getSession();
	boolean isVeto();
	Object getInstance();
	String getEntityName();
	Object[] getState();
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
	 * @see org.hibernate.action.internal.AbstractEntityInsertAction#nullifyTransientReferencesIfNotAlready()
	 * @see #rxMakeEntityManaged()
	 */
	default CompletionStage<Void> rxNullifyTransientReferencesIfNotAlready() {
		if ( !areTransientReferencesNullified() ) {
			return new ForeignKeys.Nullifier( getInstance(), false, isEarlyInsert(), (SessionImplementor) getSession(), getPersister() )
					.nullifyTransientReferences( getState() ).thenAccept( v-> {
						new Nullability( getSession() ).checkNullability( getState(), getPersister(), false );
						setTransientReferencesNullified();
					} );
		}
		else {
			return RxUtil.nullFuture();
		}
	}

	/**
	 * Make the entity "managed" by the persistence context.
	 *
	 * @see org.hibernate.action.internal.AbstractEntityInsertAction#makeEntityManaged()
	 */
	default CompletionStage<Void> rxMakeEntityManaged() {
		return rxNullifyTransientReferencesIfNotAlready()
				.thenAccept( v -> getSession().getPersistenceContextInternal().addEntity(
						getInstance(),
						( getPersister().isMutable() ? Status.MANAGED : Status.READ_ONLY ),
						getState(),
						getEntityKey(),
						Versioning.getVersion( getState(), getPersister() ),
						LockMode.WRITE,
						isExecuted(),
						getPersister(),
						isVersionIncrementDisabled()
				));
	}
}
