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
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.dialect.spi.DatabaseMetaDataDialectResolutionInfoAdapter;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentInitiator;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveJdbcEnvironment;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import io.vertx.sqlclient.spi.DatabaseMetadata;

import static org.hibernate.reactive.util.impl.CompletionStages.failedFuture;

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
	public JdbcEnvironment initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		boolean explicitDialect = configurationValues.containsKey( Settings.DIALECT );
		if ( explicitDialect ) {
			DialectFactory dialectFactory = registry.getService( DialectFactory.class );
			Dialect dialect = dialectFactory.buildDialect( configurationValues, null );
			return new ReactiveJdbcEnvironment( registry, dialect );
		}

		return new ReactiveJdbcEnvironment( registry, new DialectBuilder( configurationValues, registry ).build() );
	}

	private static class DialectBuilder {

		private final Map configurationValues;
		private final ServiceRegistry registry;

		public DialectBuilder(Map configurationValues, ServiceRegistry registry) {
			this.configurationValues = configurationValues;
			this.registry = registry;
		}

		public Dialect build() {
			DialectFactory dialectFactory = registry.getService( DialectFactory.class );
			return dialectFactory.buildDialect( configurationValues, this::dialectResolutionInfo );
		}

		private DialectResolutionInfo dialectResolutionInfo() {
			ReactiveConnectionPool connectionPool = registry.getService( ReactiveConnectionPool.class );
			return connectionPool.getConnection()
					.thenCompose( connection -> {
						try {
							ReactiveDialectResolutionInfo info = new ReactiveDialectResolutionInfo( connection.getDatabaseMetadata() );
							return connection.close().thenApply( v -> info );
						}
						catch (Throwable t) {
							return connection.close()
									.handle( CompletionStages::handle )
									// Ignore errors when closing the connection
									.thenCompose( handled -> failedFuture( t ) );
						}
					} ).toCompletableFuture().join();
		}
	}

	private static class ReactiveDialectResolutionInfo implements DialectResolutionInfo {

		private final DatabaseMetadata metadata;

		private ReactiveDialectResolutionInfo(DatabaseMetadata metadata) {
			this.metadata = metadata;
		}

		@Override
		public String getDatabaseName() {
			return metadata.productName();
		}

		@Override
		public String getDatabaseVersion() {
			return metadata.fullVersion();
		}

		@Override
		public int getDatabaseMajorVersion() {
			return metadata.majorVersion();
		}

		@Override
		public int getDatabaseMinorVersion() {
			return metadata.minorVersion();
		}

		@Override
		public String getDriverName() {
			return getDatabaseName();
		}

		@Override
		public int getDriverMajorVersion() {
			return getDatabaseMajorVersion();
		}

		@Override
		public int getDriverMinorVersion() {
			return getDatabaseMinorVersion();
		}

		@Override
		public String getSQLKeywords() {
			return null;
		}
	}

	private JdbcEnvironment createJdbcEnvironmentWithMetadata(
			Map configurationValues,
			ServiceRegistryImplementor registry) {
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
}
