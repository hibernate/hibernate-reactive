package org.hibernate.reactive.loader.collection.impl;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.loader.collection.CollectionInitializer;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

public interface ReactiveCollectionInitializer extends CollectionInitializer {
	CompletionStage<Void> reactiveInitialize(Serializable id, SharedSessionContractImplementor session);
}
