/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bulk.impl;

import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A {@link ParameterSpecification} that just automatically binds the
 * {@link SharedSessionContractImplementor#getSessionIdentifier() session id}.
 *
 * @author Gavin King
 */
class SessionIdParameterSpecification implements ParameterSpecification {

    static final ParameterSpecification SESSION_ID = new SessionIdParameterSpecification();

    @Override
    public Type getExpectedType() {
        return StandardBasicTypes.UUID_CHAR;
    }

    @Override
    public void setExpectedType(Type expectedType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String renderDisplayInfo() {
        return "hib_sess_id";
    }

    @Override
    public int bind(PreparedStatement statement, QueryParameters qp, SharedSessionContractImplementor session, int position)
            throws SQLException {
        StandardBasicTypes.UUID_CHAR.nullSafeSet(statement, session.getSessionIdentifier(), position, session);
        return 1;
    }
}
