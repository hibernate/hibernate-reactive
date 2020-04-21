package org.hibernate.rx.engine.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.internal.StatefulPersistenceContext;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.rx.util.impl.RxUtil;

/**
 * Add reactive methods to a {@link PersistenceContext}.
 */
 /*
 * <p>
 *     This doesn't extend {@link StatefulPersistenceContext} because {@link org.hibernate.rx.impl.RxSessionInternalImpl}
 *     extends {@link org.hibernate.internal.SessionImpl} and it's not possible to pass a {@link PersistenceContext} to
 *     the super class. The result is that we would have to override all the methods in SessionImpl using the persistence context.
 * </p>
 * <p>We could solve this by adding a constructor in SessionImpl that receives a {@link PersistenceContext}.</p>
 */
public class RxPersistenceContextAdapter extends StatefulPersistenceContext {

	/**
	 * Constructs a PersistentContext, bound to the given session.
	 *
	 * @param session The session "owning" this context.
	 */
	public RxPersistenceContextAdapter(SharedSessionContractImplementor session) {
		super( session );
	}

	public CompletionStage<Void> rxInitializeNonLazyCollections() throws HibernateException {
		CompletionStage<Void> stage = RxUtil.nullFuture();
		return stage.thenAccept( v -> initializeNonLazyCollections() );
	}

}
