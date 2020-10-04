/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.Configurable;

import java.net.URI;
import java.util.Map;

import static org.hibernate.internal.CoreLogging.messageLogger;
import static org.hibernate.internal.util.config.ConfigurationHelper.getInt;
import static org.hibernate.internal.util.config.ConfigurationHelper.getInteger;
import static org.hibernate.internal.util.config.ConfigurationHelper.getString;

/**
 * The default {@link SqlClientPoolConfiguration} which configures the
 * {@link io.vertx.sqlclient.Pool} using the Hibernate ORM configuration
 * properties defined in {@link Settings}.
 * <p>
 * A custom implementation of {@code SqlClientPoolConfiguration} might
 * choose to extend this class in order to reuse its built-in
 * functionality.
 */
public class DefaultSqlClientPoolConfiguration implements SqlClientPoolConfiguration, Configurable {

    private static final int DEFAULT_POOL_SIZE = 5;

    private int poolSize;
    private Integer maxWaitQueueSize;
    private Integer connectTimeout;
    private Integer idleTimeout;
    private Integer cacheMaxSize;
    private Integer sqlLimit;
    private String user;
    private String pass;

    @Override
    public void configure(Map configuration) {
        user = getString( Settings.USER, configuration );
        pass = getString( Settings.PASS, configuration );
        poolSize = getInt( Settings.POOL_SIZE, configuration, DEFAULT_POOL_SIZE );
        maxWaitQueueSize = getInteger( Settings.POOL_MAX_WAIT_QUEUE_SIZE, configuration );
        cacheMaxSize = getInteger( Settings.PREPARED_STATEMENT_CACHE_MAX_SIZE, configuration );
        sqlLimit = getInteger( Settings.PREPARED_STATEMENT_CACHE_SQL_LIMIT, configuration );
        connectTimeout = getInteger( Settings.POOL_CONNECT_TIMEOUT, configuration );
        idleTimeout = getInteger( Settings.POOL_IDLE_TIMEOUT, configuration );
    }

    @Override
    public PoolOptions poolOptions() {
        PoolOptions poolOptions = new PoolOptions();
        messageLogger( DefaultSqlClientPool.class).infof( "HRX000012: Connection pool size: %d", poolSize );
        poolOptions.setMaxSize( poolSize );
        if (maxWaitQueueSize!=null) {
            messageLogger( DefaultSqlClientPool.class).infof( "HRX000013: Connection pool max wait queue size: %d", maxWaitQueueSize );
            poolOptions.setMaxWaitQueueSize(maxWaitQueueSize);
        }
        return poolOptions;
    }

    @Override
    public SqlConnectOptions connectOptions(URI uri) {

        String scheme = uri.getScheme();

        String database = uri.getPath().substring( 1 );
        if ( scheme.equals("db2") && database.indexOf( ':' ) > 0 ) {
            // DB2 URLs are a bit odd and have the format:
            // jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
            database = database.substring( 0, database.indexOf( ':' ) );
        }

        //see if the credentials were specified via properties
        String username = user;
        String password = pass;
        if (username==null || password==null) {
            //if not, look for URI-style user info first
            String userInfo = uri.getUserInfo();
            if (userInfo!=null) {
                String[] bits = userInfo.split(":");
                username = bits[0];
                if (bits.length>1) {
                    password = bits[1];
                }
            }
            else {
                //check the query for named parameters
                //in case it's a JDBC-style URL
                String[] params = {};
                // DB2 URLs are a bit odd and have the format:
                // jdbc:db2://<HOST>:<PORT>/<DB>:key1=value1;key2=value2;
                if ( scheme.equals("db2") ) {
                    int queryIndex = uri.getPath().indexOf(':') + 1;
                    if (queryIndex > 0) {
                        params = uri.getPath().substring(queryIndex).split(";");
                    }
                }
                else {
                    params = uri.getQuery().split("&");
                }
                for (String param : params) {
                    if ( param.startsWith("user=") ) {
                        username = param.substring(5);
                    }
                    if ( param.startsWith("pass=") ) {
                        password = param.substring(5);
                    }
                    if ( param.startsWith("password=") ) {
                        password = param.substring(9);
                    }
                }
            }
        }

        int port = uri.getPort();
        if (port==-1) {
            switch (scheme) {
                case "postgresql": case "postgres": port = 5432; break;
                case "mysql": port = 3306; break;
                case "db2": port = 50000; break;
            }
        }

        SqlConnectOptions connectOptions = new SqlConnectOptions()
                .setHost( uri.getHost() )
                .setPort( port )
                .setDatabase( database )
                .setUser( username );
        if (password != null) {
            connectOptions.setPassword( password );
        }

        //enable the prepared statement cache by default
        connectOptions.setCachePreparedStatements(true);

        if (cacheMaxSize!=null) {
            if (cacheMaxSize <= 0) {
                messageLogger( DefaultSqlClientPool.class).infof( "HRX000014: Prepared statement cache disabled", cacheMaxSize );
                connectOptions.setCachePreparedStatements(false);
            }
            else {
                messageLogger( DefaultSqlClientPool.class).infof( "HRX000015: Prepared statement cache max size: %d", cacheMaxSize );
                connectOptions.setCachePreparedStatements(true);
                connectOptions.setPreparedStatementCacheMaxSize(cacheMaxSize);
            }
        }

        if (sqlLimit!=null) {
            messageLogger( DefaultSqlClientPool.class).infof( "HRX000016: Prepared statement cache SQL limit: %d", sqlLimit );
            connectOptions.setPreparedStatementCacheSqlLimit(sqlLimit);
        }

        if (connectTimeout!=null) {
            connectOptions.setConnectTimeout(connectTimeout);
        }
        if (idleTimeout!=null) {
            connectOptions.setConnectTimeout(idleTimeout);
        }

        return connectOptions;
    }

}
