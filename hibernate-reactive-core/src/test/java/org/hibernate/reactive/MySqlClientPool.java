/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.sqlclient.Pool;
import org.hibernate.reactive.pool.impl.DefaultSqlClientPool;


public class MySqlClientPool extends DefaultSqlClientPool {
    @Override
    protected Pool getTenantPool(String tenantId) {
        return getPool();
    }
}
