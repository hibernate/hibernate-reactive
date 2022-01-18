/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hibernate.HibernateError;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.Configurable;

import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;

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

    private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

    private static final int DEFAULT_POOL_SIZE = 5;

    private int poolSize;
    private Integer maxWaitQueueSize;
    private Integer connectTimeout;
    private Integer idleTimeout;
    private Integer cacheMaxSize;
    private Integer sqlLimit;
    private Integer poolCleanerPeriod;
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
        poolCleanerPeriod = getInteger( Settings.POOL_CLEANER_PERIOD, configuration );
    }

    @Override
    public PoolOptions poolOptions() {
        PoolOptions poolOptions = new PoolOptions();

        LOG.connectionPoolSize( poolSize );
        poolOptions.setMaxSize( poolSize );
        if (maxWaitQueueSize!=null) {
            LOG.connectionPoolMaxWaitSize( maxWaitQueueSize );
            poolOptions.setMaxWaitQueueSize(maxWaitQueueSize );
        }
        if (idleTimeout!=null) {
            LOG.connectionPoolIdleTimeout( idleTimeout );
            poolOptions.setIdleTimeout(idleTimeout);
            poolOptions.setIdleTimeoutUnit(TimeUnit.MILLISECONDS);
        }
        if (connectTimeout!=null) {
            LOG.connectionPoolTimeout( connectTimeout );
            poolOptions.setConnectionTimeout(connectTimeout);
            poolOptions.setConnectionTimeoutUnit(TimeUnit.MILLISECONDS);
        }
        if (poolCleanerPeriod!=null) {
            LOG.connectionPoolCleanerPeriod( poolCleanerPeriod );
            poolOptions.setPoolCleanerPeriod(poolCleanerPeriod);
        }
        return poolOptions;
    }

    @Override
    public SqlConnectOptions connectOptions(URI uri) {
        SqlConnectOptions connectOptions;
        String originalScheme = uri.getScheme();
        URI convertedURI = ReactiveToVertxUriConverter.convertUriToVertx( uri );

        // if scheme is unsupported then vertx will throw an error
        // capture and re-throw as IllegalArgumentException

        try {
            connectOptions = SqlConnectOptions.fromUri( convertedURI.toString() );
        }
        catch (java.util.ServiceConfigurationError ex) {
            throw new IllegalArgumentException( ex );
        }

        // user property can be overridden. Check and override if necessary
        if ( user != null ) {
            connectOptions.setUser( user );
        }
        else if ( connectOptions.getUser() == null ) {
            throw new HibernateError(
                    "database username not specified (set the property 'javax.persistence.jdbc.user', or include it as a parameter in the connection URL)" );
        }

        // password property can be overridden. Check and override if necessary
        if ( pass != null ) {
            connectOptions.setPassword( user );
        }

        // port may be empty. Check and set default port if necessary based on scheme
        if ( connectOptions.getPort() == -1 ||
                ( originalScheme.equalsIgnoreCase( "cockroachdb" ) && connectOptions.getPort() == 5432 ) ) {
            connectOptions.setPort( defaultPort( originalScheme ) );
        }

        //enable the prepared statement cache by default
        connectOptions.setCachePreparedStatements( true );

        if ( cacheMaxSize != null ) {
            if ( cacheMaxSize <= 0 ) {
                LOG.preparedStatementCacheDisabled();
                connectOptions.setCachePreparedStatements( false );
            }
            else {
                LOG.preparedStatementCacheMaxSize( cacheMaxSize );
                connectOptions.setCachePreparedStatements( true );
                connectOptions.setPreparedStatementCacheMaxSize( cacheMaxSize );
            }
        }

        if ( sqlLimit != null ) {
            LOG.preparedStatementCacheSQLLimit( sqlLimit );
            connectOptions.setPreparedStatementCacheSqlLimit( sqlLimit );
        }

        return connectOptions;
    }

    private int defaultPort(String scheme) {
        switch ( scheme ) {
            case "postgresql":
            case "postgres":
                return 5432;
            case "mariadb":
            case "mysql":
                return 3306;
            case "db2":
                return 50000;
            case "cockroachdb":
                return 26257;
            case "sqlserver":
                return 1433;
            default:
                throw new IllegalArgumentException( "Unknown default port for scheme: " + scheme );
        }
    }

}
