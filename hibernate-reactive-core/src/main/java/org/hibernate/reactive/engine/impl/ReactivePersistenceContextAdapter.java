package org.hibernate.reactive.engine.impl;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.HibernateException;
import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.internal.StatefulPersistenceContext;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.reactive.impl.ReactiveSessionInternal;
import org.hibernate.reactive.impl.ReactiveSessionInternalImpl;
import org.hibernate.reactive.util.impl.CompletionStages;

/**
 * Add reactive methods to a {@link PersistenceContext}.
 */
public class ReactivePersistenceContextAdapter extends StatefulPersistenceContext {

	/**
	 * Constructs a PersistentContext, bound to the given session.
	 *
	 * @param session The session "owning" this context.
	 */
	public ReactivePersistenceContextAdapter(SharedSessionContractImplementor session) {
		super( session );
	}

	public CompletionStage<Void> reactiveInitializeNonLazyCollections() throws HibernateException {

		final NonLazyCollectionInitializer initializer = new NonLazyCollectionInitializer();
		initializeNonLazyCollections( initializer );
		return initializer.stage;
	}

	private class NonLazyCollectionInitializer implements Consumer<PersistentCollection> {
		CompletionStage<Void> stage = CompletionStages.nullFuture();

		@Override
		public void accept(PersistentCollection nonLazyCollection) {
			stage = stage.thenCompose(
					v -> ( (ReactiveSessionInternal) getSession() )
							.reactiveFetch( nonLazyCollection, true )
							.thenAccept(vv -> {})
			);
		}
	}

	/**
	 * @deprecated use {@link #reactiveInitializeNonLazyCollections} instead.
	 */
	@Deprecated
	@Override
	public void initializeNonLazyCollections() {
		throw getSession().getExceptionConverter().convert( new UnsupportedOperationException( "ReactivePersistenceContextAdapter#initializeNonLazyCollections not supported, use reactiveInitializeNonLazyCollections instead" ) );
	}
}
