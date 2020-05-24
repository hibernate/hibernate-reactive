package org.hibernate.reactive.session;

import org.hibernate.LockMode;
import org.hibernate.transform.ResultTransformer;

/**
 /**
 * An internal contract between the reactive session implementation
 * and the {@link org.hibernate.reactive.stage.Stage.Query} and
 * {@link org.hibernate.reactive.mutiny.Mutiny.Query} APIs.
 *
 * @see ReactiveSession
 *
 * @author Gavin King
 */
public interface ReactiveNativeQuery<R> extends ReactiveQuery<R> {
	ReactiveNativeQuery<R> setResultTransformer(ResultTransformer resultTransformer);
	ReactiveNativeQuery<R> addEntity(String alias, String name, LockMode read);
	ReactiveNativeQuery<R> setResultSetMapping(String resultSetMapping);
}
