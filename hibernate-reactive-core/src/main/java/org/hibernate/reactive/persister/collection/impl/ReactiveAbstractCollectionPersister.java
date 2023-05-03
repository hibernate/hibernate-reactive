/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.BatchLoaderFactory;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoaderSingleKey;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;


/**
 * Reactive version of {@link org.hibernate.persister.collection.AbstractCollectionPersister}
 */
public interface ReactiveAbstractCollectionPersister extends ReactiveCollectionPersister {

    default ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
        return ( (ReactiveConnectionSupplier) session ).getReactiveConnection();
    }

   default boolean isCollectionLoaderReusable(LoadQueryInfluencers loadQueryInfluencers) {
        // we can reuse it so long as none of the enabled influencers affect it
        return loadQueryInfluencers == LoadQueryInfluencers.NONE || getAttributeMapping().isNotAffectedByInfluencers( loadQueryInfluencers );
    }

    default CollectionLoader generateCollectionLoader(LoadQueryInfluencers loadQueryInfluencers) {
        final int batchSize = getBatchSize();
        if ( batchSize > 1 ) {
            return getFactory().getServiceRegistry()
                    .getService( BatchLoaderFactory.class )
                    .createCollectionBatchLoader( batchSize, loadQueryInfluencers, getAttributeMapping(), getFactory() );
        }
        return new ReactiveCollectionLoaderSingleKey( getAttributeMapping(), loadQueryInfluencers, getFactory() );
    }

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#recreate(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    CompletionStage<Void> reactiveRecreate(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#remove(Object, SharedSessionContractImplementor)
     */
    CompletionStage<Void> reactiveRemove(Object id, SharedSessionContractImplementor session);

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#deleteRows(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    @Override
    CompletionStage<Void> reactiveDeleteRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#insertRows(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    @Override
    CompletionStage<Void> reactiveInsertRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

    /**
     * @see org.hibernate.persister.collection.AbstractCollectionPersister#updateRows(PersistentCollection, Object, SharedSessionContractImplementor)
     */
    @Override
    CompletionStage<Void> reactiveUpdateRows(PersistentCollection<?> collection, Object id, SharedSessionContractImplementor session);

    boolean isRowDeleteEnabled();
    boolean isRowInsertEnabled();
}
