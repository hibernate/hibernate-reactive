/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.CockroachDialect;
import org.hibernate.dialect.DB2Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDBDialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.dialect.spi.DatabaseMetaDataDialectResolutionInfoAdapter;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentInitiator;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.dialect.ReactivePostgreSQLDialect;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveJdbcEnvironment;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * provides an implementation of {@link JdbcEnvironment} that infers
 * the Hibernate {@link org.hibernate.dialect.Dialect} from the JDBC URL.
 */
public class NoJdbcEnvironmentInitiator extends JdbcEnvironmentInitiator {
	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	public static final NoJdbcEnvironmentInitiator INSTANCE = new NoJdbcEnvironmentInitiator();

	@Override
	public Class<JdbcEnvironment> getServiceInitiated() {
		return JdbcEnvironment.class;
	}

	@Override
	@SuppressWarnings("unchecked")
	public JdbcEnvironment initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		boolean explicitDialect = configurationValues.containsKey( Settings.DIALECT );
		String url = configurationValues.getOrDefault( Settings.URL, "" ).toString();
		if ( !explicitDialect ) {
			Class<? extends Dialect> dialectClass = guessDialect( url );
			if ( dialectClass != null ) {
				configurationValues.put( Settings.DIALECT, dialectClass.getName() );
				explicitDialect = true;
			}
		}

		// 'hibernate.temp.use_jdbc_metadata_defaults' is a temporary magic value.
		// The need for it is intended to be alleviated with future development, thus it is
		// not defined as an Settings constant...
		//
		// it is used to control whether we should consult the JDBC metadata to determine
		// certain Settings default values; it is useful to *not* do this when the database
		// may not be available (mainly in tools usage).
		boolean useJdbcMetadata = ConfigurationHelper
				.getBoolean( "hibernate.temp.use_jdbc_metadata_defaults", configurationValues, true );

		if ( useJdbcMetadata ) {
			JdbcEnvironment jdbcEnv = createJdbcEnvironmentWithMetadata( configurationValues, registry );
			if ( jdbcEnv != null ) {
				return jdbcEnv;
			}
		}

		// if we get here, either we were asked to not use JDBC metadata or accessing the JDBC metadata failed.
		if ( explicitDialect ) {
			DialectFactory dialectFactory = registry.getService( DialectFactory.class );
			Dialect dialect = dialectFactory.buildDialect( configurationValues, null );
			return new ReactiveJdbcEnvironment( registry, dialect );
		}
		if ( url.isEmpty() ) {
			throw LOG.couldNotDetermineDialectFromJdbcDriverMetadata();
		}
		throw LOG.couldNotDetermineDialectFromConnectionURI( url );
	}

	private JdbcEnvironment createJdbcEnvironmentWithMetadata(Map configurationValues, ServiceRegistryImplementor registry) {
		DialectFactory dialectFactory = registry.getService( DialectFactory.class );
		ConnectionProvider connectionProvider = registry.getService( ConnectionProvider.class );
		try {
			final JdbcConnectionAccess jdbcConnectionAccess = new ConnectionProviderJdbcConnectionAccess( connectionProvider );
			final Connection connection = jdbcConnectionAccess.obtainConnection();
			try {
				Dialect dialect = dialect( configurationValues, dialectFactory, connection );
				return new ReactiveJdbcEnvironment( registry, dialect );
			}
			finally {
				jdbcConnectionAccess.releaseConnection( connection );
			}
		}
		catch (Throwable t) {
			LOG.tracef( "Ignoring error %s", t.getMessage() );
			return null;
		}
	}

	private static Dialect dialect(Map configurationValues, DialectFactory dialectFactory, Connection connection) {
		return dialectFactory.buildDialect( configurationValues, () -> {
			try {
				return new DatabaseMetaDataDialectResolutionInfoAdapter( connection.getMetaData() );
			}
			catch (SQLException sqlException) {
				return null;
			}
		} );
	}

	protected Class<? extends Dialect> guessDialect(String url) {
		if ( url.startsWith( "jdbc:" ) ) {
			url = url.substring( 5 );
		}

		if ( url.startsWith( "mysql:" ) ) {
			return MySQLDialect.class;
		}
		if ( url.startsWith( "mariadb:" ) ) {
			return MariaDBDialect.class;
		}
		if ( url.startsWith( "postgresql:" ) || url.startsWith( "postgres:" ) ) {
			return ReactivePostgreSQLDialect.class;
		}
		if ( url.startsWith( "db2:" ) ) {
			return DB2Dialect.class;
		}
		if ( url.startsWith( "cockroachdb:" ) ) {
			return CockroachDialect.class;
		}
		if ( url.startsWith( "sqlserver:" ) ) {
			return SQLServerDialect.class;
		}
		if ( url.startsWith( "oracle:" ) ) {
			return OracleDialect.class;
		}

		return null;
	}
}
