/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.sqlclient.Pool;
import org.hibernate.reactive.pool.impl.SqlClientPool;


public class MySqlClientPool extends SqlClientPool {
    @Override
    protected Pool getTenantPool(String tenantId) {
        return getPool();
    }
}
