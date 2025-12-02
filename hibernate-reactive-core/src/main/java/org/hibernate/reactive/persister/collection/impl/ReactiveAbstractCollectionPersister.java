/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.collection.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.collection.spi.PersistentCollection;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.ast.spi.CollectionLoader;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.query.named.NamedQueryMemento;
import org.hibernate.reactive.loader.ast.internal.ReactiveCollectionLoaderNamedQuery;
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

    /**
     * See org.hibernate.persister.collection.AbstractCollectionPersister#createNamedQueryCollectionLoader
     */
    default CollectionLoader createNamedQueryCollectionLoader(CollectionPersister persister, NamedQueryMemento namedQueryMemento) {
        return new ReactiveCollectionLoaderNamedQuery(persister, namedQueryMemento);
    }

    /**
     * See org.hibernate.persister.collection.AbstractCollectionPersister#createSingleKeyCollectionLoader
     */
    default CollectionLoader createSingleKeyCollectionLoader(LoadQueryInfluencers loadQueryInfluencers) {
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
