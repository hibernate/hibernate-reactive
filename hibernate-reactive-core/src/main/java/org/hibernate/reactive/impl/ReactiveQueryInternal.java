package org.hibernate.reactive.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.query.Query;

public interface ReactiveQueryInternal<R> extends Query<R> {

	CompletionStage<R> getReactiveSingleResult();

	CompletionStage<List<R>> getReactiveResultList();

	CompletionStage<Integer> executeReactiveUpdate();

	CompletionStage<List<R>> reactiveList();
}
