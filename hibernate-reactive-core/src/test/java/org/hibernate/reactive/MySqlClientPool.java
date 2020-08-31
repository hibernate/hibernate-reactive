/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.SqlClientPool;

import java.util.concurrent.CompletionStage;

public class MySqlClientPool extends SqlClientPool {
    @Override
    public CompletionStage<ReactiveConnection> getConnection(String tenantId) {
        return getConnection();
    }
}
