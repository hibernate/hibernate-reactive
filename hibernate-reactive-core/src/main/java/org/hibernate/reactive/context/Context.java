/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.context;

import org.hibernate.service.Service;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Abstracts away from the Vert.x {@link io.vertx.core.Context}
 * object, enabling alternative strategies for associating state
 * with the current reactive stream.
 *
 * @author Gavin King
 */
public interface Context extends Executor, Service {

    /**
     * Associate a value with the current reactive stream.
     */
    <T> void put(Key<T> key, T instance);

    /**
     * Get a value associated with the current reactive stream,
     * or return null.
     */
    <T> T get(Key<T> key);

    /**
     * Remove a value associated with the current reactive stream.
     */
    void remove(Key<?> key);

    /**
     * Run the given command in a context.
     *
     * If there is a context already associated with the call, use
     * that one. Otherwise, create a new context and execute the
     * command in the new context.
     */
    @Override
    void execute(Runnable runnable);

    interface Key<T> {

    }

    final class BaseKey<T> implements Key<T> {
        private final Class<T> type;
        private final String id;
        private final int hash;

        public BaseKey(Class<T> type, String id) {
            Objects.requireNonNull( type );
            Objects.requireNonNull( id );
            this.type = type;
            this.id = id;
            this.hash = id.hashCode() * 31 + type.hashCode();
        }

        @Override
        public boolean equals(Object object) {
            if ( this == object ) {
                return true;
            }
            if ( !( object instanceof BaseKey ) ) {
                return false;
            }
            BaseKey<?> key = (BaseKey<?>) object;
            return Objects.equals( id, key.id )
                && Objects.equals( type, key.type );
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    final class MultitenantKey<T> implements Key<T> {
        final BaseKey<T> base;
        final String tenantId;

        public MultitenantKey(BaseKey<T> base, String tenantId) {
            Objects.requireNonNull( base );
            Objects.requireNonNull( tenantId );
            this.base = base;
            this.tenantId = tenantId;
        }

        @Override
        public boolean equals(Object o) {
            if ( this == o ) {
                return true;
            }
            if ( !( o instanceof MultitenantKey ) ) {
                return false;
            }

            MultitenantKey<?> that = (MultitenantKey<?>) o;

            if ( !base.equals( that.base ) ) {
                return false;
            }
            return tenantId.equals( that.tenantId );
        }

        @Override
        public int hashCode() {
            int result = base.hashCode();
            result = 31 * result + tenantId.hashCode();
            return result;
        }
    }

}
