/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDB103Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.junit.Rule;
import org.junit.Test;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;


public class UriConfigTest extends BaseReactiveTest {

    @Rule // Storing UUID doesn't work with DB2
    public DatabaseSelectionRule dbRule = DatabaseSelectionRule.skipTestsFor( DB2, MARIA );

    @Override
    protected Configuration constructConfiguration() {
        Class<? extends Dialect> dialect;
        switch ( DatabaseConfiguration.dbType() ) {
            case POSTGRESQL: dialect = PostgreSQL10Dialect.class; break;
            case MYSQL: dialect = MySQL8Dialect.class; break;
            case MARIA: dialect = MariaDB103Dialect.class; break;
            case DB2:
            default: throw new IllegalArgumentException();
        }
        Configuration configuration = super.constructConfiguration();
        configuration.setProperty( Environment.DIALECT, dialect.getName() );
        configuration.setProperty( Settings.URL, DatabaseConfiguration.getUri() );
        configuration.setProperty( Settings.SQL_CLIENT_POOL_CONFIG, UriPoolConfiguration.class.getName() );
        return configuration;
    }

    @Test
    public void testUriConfig(TestContext context) {
        String sql;
        switch ( DatabaseConfiguration.dbType() ) {
            case POSTGRESQL:
                sql = "select cast(current_timestamp as varchar)"; break;
            case MYSQL:
            case MARIA:
                sql = "select cast(current_timestamp as char) from dual"; break;
            case DB2:
            default: throw new IllegalArgumentException();
        }
        test(context,
                getSessionFactory()
                        .withTransaction( (s,t) -> s.createNativeQuery( sql, String.class ).getSingleResult() )
                        .thenAccept( ts -> context.assertTrue(ts instanceof String) )
        );
    }
}
