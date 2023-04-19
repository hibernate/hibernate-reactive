/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.SQLServerDialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.reactive.engine.jdbc.env.internal.ReactiveJdbcEnvironment;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LogCategory;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import io.vertx.sqlclient.spi.DatabaseMetadata;

import static java.util.function.Function.identity;
import static org.hibernate.reactive.logging.impl.LoggerFactory.make;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * provides an implementation of {@link JdbcEnvironment} that infers
 * the Hibernate {@link org.hibernate.dialect.Dialect} from the JDBC URL.
 */
public class NoJdbcEnvironmentInitiator implements StandardServiceInitiator<JdbcEnvironment> {

	/**
	 * I'm using the same logger category used in {@link org.hibernate.engine.jdbc.dialect.internal.DialectFactoryImpl}.
	 */
	private static final Log LOG = make( Log.class, new LogCategory( "SQL dialect" ) );

	public static final NoJdbcEnvironmentInitiator INSTANCE = new NoJdbcEnvironmentInitiator();

	@Override
	public Class<JdbcEnvironment> getServiceInitiated() {
		return JdbcEnvironment.class;
	}

	@Override
	public JdbcEnvironment initiateService(Map<String, Object> configurationValues, ServiceRegistryImplementor registry) {
		boolean explicitDialect = configurationValues.containsKey( Settings.DIALECT );
		if ( explicitDialect ) {
			DialectFactory dialectFactory = registry.getService( DialectFactory.class );
			Dialect dialect = dialectFactory.buildDialect( configurationValues, null );
			return new ReactiveJdbcEnvironment( registry, dialect );
		}

		return new ReactiveJdbcEnvironment( registry, new DialectBuilder( configurationValues, registry ).build() );
	}

	private static class DialectBuilder {

		private final Map<String, Object> configurationValues;
		private final ServiceRegistry registry;

		public DialectBuilder(Map<String, Object> configurationValues, ServiceRegistry registry) {
			this.configurationValues = configurationValues;
			this.registry = registry;
		}

		public Dialect build() {
			DialectFactory dialectFactory = registry.getService( DialectFactory.class );
			Dialect dialect = dialectFactory.buildDialect( configurationValues, this::dialectResolutionInfo );
			return checkDialect( dialect );
		}

		/**
		 * Workaround for <a href="https://github.com/eclipse-vertx/vertx-sql-client/issues/1312">vertx-sql-client#1312</a>
		 * <p>
		 * For extracting the information about sequences, the Sql Server dialect 11+ uses the following query:
		 * <pre>{@code select * from INFORMATION_SCHEMA.SEQUENCES}</pre>
		 * But, the Vert.x MSSQL client throws an exception when running it.
		 */
		private static Dialect checkDialect(Dialect dialect) {
			if ( dialect instanceof SQLServerDialect && dialect.getVersion().isSameOrAfter( 11 ) ) {
				SQLServerDialect sqlServerDialect = new SQLServerDialect( DatabaseVersion.make( 10 ) );
				LOG.replacingDialect( dialect, sqlServerDialect );
				return sqlServerDialect;
			}
			return dialect;
		}

		private DialectResolutionInfo dialectResolutionInfo() {
			ReactiveConnectionPool connectionPool = registry.getService( ReactiveConnectionPool.class );
			return connectionPool
					// The default SqlExceptionHelper in ORM requires the dialect, but we haven't created a dialect yet,
					// so we need to override it at this stage, or we will have an exception.
					.getConnection( new SqlExceptionHelper( true ) )
					.thenCompose( DialectBuilder::buildResolutionInfo )
					.toCompletableFuture().join();
		}

		private static CompletionStage<ReactiveDialectResolutionInfo> buildResolutionInfo(ReactiveConnection connection) {
			final DatabaseMetadata databaseMetadata = connection.getDatabaseMetadata();
			return resolutionInfoStage( connection, databaseMetadata )
					.handle( CompletionStages::handle )
					.thenCompose( handled -> {
						if ( handled.hasFailed() ) {
							// Something has already gone wrong: try to close the connection
							// and return the original failure
							return connection.close()
									.handle( (unused, throwable) -> handled.getResultAsCompletionStage() )
									.thenCompose( identity() );
						}
						else {
							return connection.close()
									.thenCompose( v -> handled.getResultAsCompletionStage() );
						}
					} );
		}

		private static CompletionStage<ReactiveDialectResolutionInfo> resolutionInfoStage(ReactiveConnection connection, DatabaseMetadata databaseMetadata) {
			if ( databaseMetadata.productName().equalsIgnoreCase( "PostgreSQL" ) ) {
				// We need to check if the database is PostgreSQL or CockroachDB
				// Hibernate ORM does it using a query, so we need to check in advance
				// See org.hibernate.dialect.Database.POSTGRESQL#createDialect
				return connection.select( "select version()" )
						.thenApply( DialectBuilder::readFullVersion )
						.thenApply( fullversion -> {
							if ( fullversion.startsWith( "Cockroach" ) ) {
								return new CockroachDatabaseMetadata( fullversion );
							}
							return databaseMetadata;
						} )
						.thenApply( ReactiveDialectResolutionInfo::new );
			}

			return completedFuture( new ReactiveDialectResolutionInfo( databaseMetadata ) );
		}

		private static String readFullVersion(ReactiveConnection.Result result) {
			return result.hasNext()
					? (String) result.next()[0]
					: "";
		}
	}

	private static class CockroachDatabaseMetadata implements DatabaseMetadata {

		private final String fullversion;

		public CockroachDatabaseMetadata(String fullversion) {
			this.fullversion = fullversion;
		}

		@Override
		public String productName() {
			return "CockroachDb";
		}

		@Override
		public String fullVersion() {
			return fullversion;
		}

		@Override
		public int majorVersion() {
			return 0;
		}

		@Override
		public int minorVersion() {
			return 0;
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

		@Override
		public String toString() {
			return getDatabaseMajorVersion() + "." + getDatabaseMinorVersion();
		}
	}
}
