/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.engine.jdbc.connections.spi.AbstractMultiTenantConnectionProvider;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;

/**
 * A dummy Hibernate
 * {@link org.hibernate.engine.jdbc.connections.spi.MultiTenantConnectionProvider}
 * which throws an exception if a JDBC connection is requested.
 *
 * @author Gavin King
 */
public class NoJdbcMultiTenantConnectionProvider extends AbstractMultiTenantConnectionProvider {
    @Override
    protected ConnectionProvider getAnyConnectionProvider() {
        throw new UnsupportedOperationException("Not using JDBC");
    }

    @Override
    protected ConnectionProvider selectConnectionProvider(String tenantIdentifier) {
        throw new UnsupportedOperationException("Not using JDBC");
    }
}
