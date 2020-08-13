/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.internal.ast.HqlSqlWalker;
import org.hibernate.hql.spi.id.MultiTableBulkIdStrategy;

/**
 * An empty implementation of {@link MultiTableBulkIdStrategy} that avoids hitting
 * the database via JDBC in order to create a temporary table.
 *
 * @author Gavin King
 */
class NoJdbcMultiTableBulkIdStrategy implements MultiTableBulkIdStrategy {
    @Override
    public void prepare(JdbcServices jdbcServices, JdbcConnectionAccess connectionAccess,
                        MetadataImplementor metadata, SessionFactoryOptions sessionFactoryOptions) {}

    @Override
    public void release(JdbcServices jdbcServices, JdbcConnectionAccess connectionAccess) {}

    @Override
    public UpdateHandler buildUpdateHandler(SessionFactoryImplementor factory, HqlSqlWalker walker) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DeleteHandler buildDeleteHandler(SessionFactoryImplementor factory, HqlSqlWalker walker) {
        throw new UnsupportedOperationException();
    }
}
