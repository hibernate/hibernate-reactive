package org.hibernate.reactive.session;

import org.hibernate.LockMode;
import org.hibernate.jpa.spi.NativeQueryTupleTransformer;

/**
 * @author Gavin King
 */
public interface ReactiveNativeQuery<R> extends ReactiveQuery<R> {
	ReactiveNativeQuery<R> setResultTransformer(NativeQueryTupleTransformer nativeQueryTupleTransformer);
	ReactiveNativeQuery<R> addEntity(String alias, String name, LockMode read);
	ReactiveNativeQuery<R> setResultSetMapping(String resultSetMapping);
}
