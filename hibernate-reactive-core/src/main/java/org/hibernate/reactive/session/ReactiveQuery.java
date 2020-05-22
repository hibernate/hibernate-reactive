package org.hibernate.reactive.session;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.Incubating;
import org.hibernate.query.Query;

/**
 * An internal contract between the reactive session implementation
 * and the {@link org.hibernate.reactive.stage.Stage.Query} and
 * {@link org.hibernate.reactive.mutiny.Mutiny.Query} APIs.
 *
 * @see ReactiveSession
 */
@Incubating
public interface ReactiveQuery<R> extends Query<R> {

	CompletionStage<R> getReactiveSingleResult();

	CompletionStage<List<R>> getReactiveResultList();

	CompletionStage<Integer> executeReactiveUpdate();

	CompletionStage<List<R>> reactiveList();
}
