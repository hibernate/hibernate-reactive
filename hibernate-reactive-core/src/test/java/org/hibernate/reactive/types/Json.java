/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import io.vertx.core.json.JsonObject;
import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;

public class Json implements UserType {

    @Override
    public int[] sqlTypes() {
        return new int[] {Types.OTHER};
    }

    @Override
    public Class returnedClass() {
        return JsonObject.class;
    }

    @Override
    public boolean equals(Object x, Object y) throws HibernateException {
        return Objects.equals(x,y);
    }

    @Override
    public int hashCode(Object x) throws HibernateException {
        return Objects.hashCode(x);
    }

    @Override
    public Object nullSafeGet(ResultSet rs, String[] names, SharedSessionContractImplementor session, Object owner) throws HibernateException, SQLException {
        Object rsObject = rs.getObject( names[0] );
        // Currently Vertx does not return JsonObject type from MariaDb so adding type check to convert the String value
        if ( rsObject instanceof String ) {
            return new JsonObject( rsObject.toString() );
        }
        return rsObject;
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Object value, int index, SharedSessionContractImplementor session) throws HibernateException, SQLException {
        if (value==null) {
            st.setNull(index, Types.OTHER);
        }
        else {
            st.setObject(index, value);
        }
    }

    @Override
    public Object deepCopy(Object value) throws HibernateException {
        return value==null ? null : ((JsonObject) value).copy();
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Serializable disassemble(Object value) throws HibernateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object assemble(Serializable cached, Object owner) throws HibernateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object replace(Object original, Object target, Object owner) throws HibernateException {
        throw new UnsupportedOperationException();
    }
}
