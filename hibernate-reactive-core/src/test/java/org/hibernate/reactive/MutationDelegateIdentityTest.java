/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.RowId;
import org.hibernate.annotations.SourceType;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.id.insert.AbstractReturningDelegate;
import org.hibernate.id.insert.AbstractSelectingDelegate;
import org.hibernate.id.insert.UniqueKeySelectingDelegate;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.sql.model.MutationType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;

/**
 * Inspired by the test
 * {@code org.hibernate.orm.test.mapping.generated.delegate.MutationDelegateIdentityTest}
 * in Hibernate ORM.
 */
public class MutationDelegateIdentityTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of(
				IdentityOnly.class,
				IdentityAndValues.class,
				IdentityAndValuesAndRowId.class,
				IdentityAndValuesAndRowIdAndNaturalId.class
		);
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		// Batch size is only enabled to make sure it's ignored when using mutation delegates
		configuration.setProperty( AvailableSettings.STATEMENT_BATCH_SIZE, "5" );

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand off any actual logging properties
		sqlTracker = new SqlStatementTracker( s -> true, configuration.getProperties() );
		return configuration;
	}

	@BeforeEach
	public void clearTracker() {
		sqlTracker.clear();
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	@Test
	public void testInsertGeneratedIdentityOnly(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( IdentityOnly.class, MutationType.INSERT );
		final int expectedQueriesSize = delegate instanceof AbstractReturningDelegate ? 1 : 2;
		final IdentityOnly entity = new IdentityOnly();

		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> {
					assertThat( entity.getId() ).isNotNull();
					assertThat( entity.getName() ).isNull();
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
				} )
	  	) );
	}

	@Test
	public void testInsertGeneratedValuesAndIdentity(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( IdentityAndValues.class, MutationType.INSERT );
		final int expectedQueriesSize = delegate instanceof AbstractSelectingDelegate
				? 3
				: delegate != null && delegate.supportsArbitraryValues() ? 1 : 2;
		final IdentityAndValues entity = new IdentityAndValues();

		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> {
					assertThat( entity.getId() ).isNotNull();
					assertThat( entity.getName() ).isEqualTo( "default_name" );
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
				} )
	  	) );
	}

	@Test
	public void testUpdateGeneratedValuesAndIdentity(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( IdentityAndValues.class, MutationType.UPDATE );
		final IdentityAndValues entity = new IdentityAndValues();
		final int expectedQuerySize = delegate != null && delegate.supportsArbitraryValues() ? 3 : 4;

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( entity ) )
				.invoke( sqlTracker::clear )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( IdentityAndValues.class, entity.getId() )
						.invoke( identityAndValues -> identityAndValues.setData( "changed" ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( IdentityAndValues.class, entity.getId() )
						.invoke( identityAndValues -> {
							assertThat( entity.getUpdateDate() ).isNotNull();
							assertThat( sqlTracker.getLoggedQueries().size() ).isEqualTo( expectedQuerySize );
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
									.startsWith( "select" ).contains( "update" );
						} )
				) )
		);
	}

	@Test
	@DisabledFor(value = ORACLE, reason = "Vert.x driver doesn't support RowId type parameters")
	public void testInsertGeneratedValuesAndIdentityAndRowId(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( IdentityAndValuesAndRowId.class, MutationType.INSERT );
		final int expectedQueriesSize = delegate instanceof AbstractSelectingDelegate
				? 3
				: delegate != null && delegate.supportsArbitraryValues() ? 1 : 2;
		final boolean shouldHaveRowId = delegate != null && delegate.supportsRowId() && getDialect().rowId( "" ) != null;
		final IdentityAndValuesAndRowId entity = new IdentityAndValuesAndRowId();

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.persist( entity )
						.call( s::flush )
						.invoke( () -> {
							assertThat( entity.getId() ).isNotNull();
							assertThat( entity.getName() ).isEqualTo( "default_name" );
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
							if ( shouldHaveRowId ) {
								// assert row-id was populated in entity entry
								final PersistenceContext pc = ( (MutinySessionImpl) s )
										.unwrap( ReactiveSession.class )
										.getPersistenceContext();
								final EntityEntry entry = pc.getEntry( entity );
								assertThat( entry.getRowId() ).isNotNull();
							}
							sqlTracker.clear();
							entity.setData( "changed" );
						} )
						.call( s::flush )
						.invoke( () -> {
							assertThat( entity.getUpdateDate() ).isNotNull();
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "update" );
							assertNumberOfOccurrenceInQueryNoSpace( 0, "id_column", shouldHaveRowId ? 0 : 1 );
						} )
				)
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( IdentityAndValuesAndRowId.class, entity.getId() )
						.invoke( identityAndValuesAndRowId -> assertThat( identityAndValuesAndRowId.getUpdateDate() ).isNotNull() )
			 ) )
		);
	}

	@Test
	@DisabledFor(value = ORACLE, reason = "Vert.x driver doesn't support RowId type parameters")
	public void testInsertGeneratedValuesAndIdentityAndRowIdAndNaturalId(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( IdentityAndValuesAndRowIdAndNaturalId.class, MutationType.INSERT );
		final IdentityAndValuesAndRowIdAndNaturalId entity = new IdentityAndValuesAndRowIdAndNaturalId( "naturalid_1" );
		final boolean isUniqueKeyDelegate = delegate instanceof UniqueKeySelectingDelegate;
		final int expectedQueriesSize = delegate == null || !delegate.supportsArbitraryValues() || isUniqueKeyDelegate ? 2 : 1;
		test(context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> {
					assertThat( entity.getId() ).isNotNull();
					assertThat( entity.getName() ).isEqualTo( "default_name" );
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );

					if ( isUniqueKeyDelegate ) {
						assertNumberOfOccurrenceInQueryNoSpace( 1, "data", 1 );
						assertNumberOfOccurrenceInQueryNoSpace( 1, "id_column", 1 );
					}

					final boolean shouldHaveRowId = delegate != null && delegate.supportsRowId() && getDialect().rowId( "" ) != null;
					if ( shouldHaveRowId ) {
						// assert row-id was populated in entity entry
						final PersistenceContext pc = ( (MutinySessionImpl) s )
								.unwrap( ReactiveSession.class )
								.getPersistenceContext();
						final EntityEntry entry = pc.getEntry( entity );
						assertThat( entry.getRowId() ).isNotNull();
					}
				} ) )
		);
	}

	private static void assertNumberOfOccurrenceInQueryNoSpace(int queryNumber, String toCheck, int expectedNumberOfOccurrences) {
		String query = sqlTracker.getLoggedQueries().get( queryNumber );
		int actual = query.split( toCheck, -1 ).length - 1;
		assertThat( actual ).as( "number of " + toCheck ).isEqualTo( expectedNumberOfOccurrences );
	}

	private static GeneratedValuesMutationDelegate getDelegate(Class<?> entityClass, MutationType mutationType) {
		return ( (SessionFactoryImplementor) factoryManager
				.getHibernateSessionFactory() )
				.getMappingMetamodel()
				.findEntityDescriptor( entityClass )
				.getMutationDelegate( mutationType );
	}

	@Entity(name = "IdentityOnly")
	public static class IdentityOnly {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		private String name;

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}

	@Entity(name = "IdentityAndValues")
	@SuppressWarnings("unused")
	public static class IdentityAndValues {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		@Generated(event = EventType.INSERT)
		@ColumnDefault("'default_name'")
		private String name;

		@UpdateTimestamp(source = SourceType.DB)
		private Date updateDate;

		private String data;

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Date getUpdateDate() {
			return updateDate;
		}

		public void setData(String data) {
			this.data = data;
		}
	}

	@RowId
	@Entity(name = "IdentityAndValuesAndRowId")
	@SuppressWarnings("unused")
	public static class IdentityAndValuesAndRowId {
		@Id
		@Column(name = "id_column")
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		@Generated(event = EventType.INSERT)
		@ColumnDefault("'default_name'")
		private String name;

		@UpdateTimestamp(source = SourceType.DB)
		private Date updateDate;

		private String data;

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public Date getUpdateDate() {
			return updateDate;
		}

		public void setData(String data) {
			this.data = data;
		}
	}

	@RowId
	@Entity(name = "IdentityAndValuesAndRowIdAndNaturalId")
	@SuppressWarnings("unused")
	public static class IdentityAndValuesAndRowIdAndNaturalId {
		@Id
		@Column(name = "id_column")
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		@Generated(event = EventType.INSERT)
		@ColumnDefault("'default_name'")
		private String name;

		@NaturalId
		private String data;

		public IdentityAndValuesAndRowIdAndNaturalId() {
		}

		private IdentityAndValuesAndRowIdAndNaturalId(String data) {
			this.data = data;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}
	}
}
