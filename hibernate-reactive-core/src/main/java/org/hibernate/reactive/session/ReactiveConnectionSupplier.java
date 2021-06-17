/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session;

import org.hibernate.Incubating;
import org.hibernate.reactive.pool.ReactiveConnection;

/**
 * A source of {@link ReactiveConnection}s.
 */
@Incubating
public interface ReactiveConnectionSupplier {
    /**
     * Obtain the {@link ReactiveConnection} that is associated with the current session.
     */
    ReactiveConnection getReactiveConnection();
}
