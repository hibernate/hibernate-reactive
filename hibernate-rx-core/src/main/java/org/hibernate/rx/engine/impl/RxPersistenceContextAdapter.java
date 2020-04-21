package org.hibernate.rx.engine.impl;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.StatefulPersistenceContext;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.rx.impl.RxSessionInternalImpl;
import org.hibernate.rx.util.impl.RxUtil;

/**
 * Add reactive methods to a {@link PersistenceContext}.
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

		final NonLazyCollectionInitializer initializer = new NonLazyCollectionInitializer();
		initializeNonLazyCollections( initializer );
		return initializer.stage;
	}

	private class NonLazyCollectionInitializer implements Consumer<PersistentCollection> {
		CompletionStage<Void> stage = RxUtil.nullFuture();

		@Override
		public void accept(PersistentCollection nonLazyCollection) {
			stage = stage.thenCompose(v -> ((RxSessionInternalImpl) getSession()).unwrap(RxSessionInternal.class)
					.rxFetch( nonLazyCollection, true )
					.thenAccept(vv -> {}));
		}
	}

	/**
	 * @deprecated use {@link #rxInitializeNonLazyCollections} instead.
	 */
	@Deprecated
	@Override
	public void initializeNonLazyCollections() {
		throw getSession().getExceptionConverter().convert( new UnsupportedOperationException( "RxPersistenceContextAdapter#initializeNonLazyCollections not supported, use rxInitializeNonLazyCollections instead" ) );
	}
}
