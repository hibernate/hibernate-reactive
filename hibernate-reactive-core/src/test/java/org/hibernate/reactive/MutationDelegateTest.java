/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

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
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.id.insert.ReactiveUniqueKeySelectingDelegate;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.sql.model.MutationType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;

/**
 * Inspired by the test
 * {@code org.hibernate.orm.test.mapping.generated.delegate.MutationDelegateTest}
 * in Hibernate ORM.
 */
public class MutationDelegateTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ValuesOnly.class, ValuesAndRowId.class, ValuesAndNaturalId.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		// Batch size is only enabled to make sure it's ignored when using mutation delegates
		configuration.setProperty( AvailableSettings.STATEMENT_BATCH_SIZE, "5");

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
	public void testInsertGeneratedValues(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( ValuesOnly.class, MutationType.INSERT );
		final int expectedQueriesSize = delegate != null && delegate.supportsArbitraryValues() ? 1 : 2;

		ValuesOnly entity = new ValuesOnly( 1L );
		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> {
					assertThat( entity.getName() ).isEqualTo( "default_name" );
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
				} ) )
		);
	}

	@Test
	public void testUpdateGeneratedValues(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( ValuesOnly.class, MutationType.UPDATE );
		final int expectedQueriesSize = delegate != null && delegate.supportsArbitraryValues() ? 3 : 4;
		final ValuesOnly entity = new ValuesOnly( 2L );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( entity ) )
				.invoke( () -> sqlTracker.clear() )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( ValuesOnly.class, 2 )
						.invoke( valuesOnly -> valuesOnly.setData( "changed" ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( ValuesOnly.class, 2 ) )
						.invoke( valuesOnly -> {
							assertThat( entity.getUpdateDate() ).isNotNull();
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
									.startsWith( "select" )
									.contains( "update" );
						} )
				)
		);
	}

	@Test
	@DisabledFor(value = ORACLE, reason = "Vert.x driver doesn't support RowId type parameters")
	public void testGeneratedValuesAndRowId(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( ValuesAndRowId.class, MutationType.INSERT );
		final int expectedQueriesSize = delegate != null && delegate.supportsArbitraryValues() ? 1 : 2;
		final boolean shouldHaveRowId = delegate != null && delegate.supportsRowId() && getDialect().rowId( "" ) != null;

		final ValuesAndRowId entity = new ValuesAndRowId( 1L );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.persist( entity )
						.call( s::flush )
						.invoke( () -> {
							assertThat( entity.getName() ).isEqualTo( "default_name" );
							assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
							if ( shouldHaveRowId ) {
								// assert row-id was populated in entity entry
								final PersistenceContext pc = ( (MutinySessionImpl) s )
										.unwrap( ReactiveSession.class ).getPersistenceContext();
								final EntityEntry entry = pc.getEntry( entity );
								assertThat( entry.getRowId() ).isNotNull();
							}
							sqlTracker.clear();
							entity.setData( "changed" );
						} )
						.call( s::flush )
						.invoke( () -> {
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "update" );
							assertNumberOfOccurrenceInQueryNoSpace( 0, "id_column", shouldHaveRowId ? 0 : 1 );
						} )
				)
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( ValuesAndRowId.class, 1 )
						.invoke( valuesAndRowId -> assertThat( valuesAndRowId.getUpdateDate() ).isNotNull() )
				) )
		);
	}

	@Test
	public void testInsertGeneratedValuesAndNaturalId(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( ValuesAndNaturalId.class, MutationType.INSERT );
		final boolean isUniqueKeyDelegate = delegate instanceof ReactiveUniqueKeySelectingDelegate;
		int expectedQueriesSize = delegate == null || isUniqueKeyDelegate ? 2 : 1;
		final ValuesAndNaturalId entity = new ValuesAndNaturalId( 1L, "natural_1" );

		test( context, getMutinySessionFactory().withTransaction( s -> s
					  .persist( entity )
					  .chain( s::flush )
					  .invoke( () -> {
						  assertThat( entity.getName() ).isEqualTo( "default_name" );
						  assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
						  assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
						  if ( isUniqueKeyDelegate ) {
							  assertNumberOfOccurrenceInQueryNoSpace( 1, "data", 1 );
							  assertNumberOfOccurrenceInQueryNoSpace( 1, "id_column", 0 );
						  }
					  } )
			  )
		);
	}

	private static void assertNumberOfOccurrenceInQueryNoSpace(int queryNumber, String toCheck, int expectedNumberOfOccurrences) {
		String query = sqlTracker.getLoggedQueries().get( queryNumber );
		int actual = query.split( toCheck, -1 ).length - 1;
		assertThat( actual ).as( "number of " + toCheck ).isEqualTo( expectedNumberOfOccurrences );
	}

	private static GeneratedValuesMutationDelegate getDelegate(Class<?> entityClass, MutationType mutationType) {
		return  ( (SessionFactoryImplementor) factoryManager
				.getHibernateSessionFactory() )
				.getMappingMetamodel()
				.findEntityDescriptor( entityClass )
				.getMutationDelegate( mutationType );
	}

	@Entity( name = "ValuesOnly" )
	public static class ValuesOnly {
		@Id
		private Long id;

		@Generated( event = EventType.INSERT )
		@ColumnDefault( "'default_name'" )
		private String name;

		@UpdateTimestamp( source = SourceType.DB )
		private Date updateDate;

		@SuppressWarnings( "FieldCanBeLocal" )
		private String data;

		public ValuesOnly() {
		}

		private ValuesOnly(Long id) {
			this.id = id;
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
	@Entity( name = "ValuesAndRowId" )
	public static class ValuesAndRowId {
		@Id
		@Column( name = "id_column" )
		private Long id;

		@Generated( event = EventType.INSERT )
		@ColumnDefault( "'default_name'" )
		private String name;

		@UpdateTimestamp( source = SourceType.DB )
		private Date updateDate;

		@SuppressWarnings( "FieldCanBeLocal" )
		private String data;

		public ValuesAndRowId() {
		}

		private ValuesAndRowId(Long id) {
			this.id = id;
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

	@Entity( name = "ValuesAndNaturalId" )
	public static class ValuesAndNaturalId {
		@Id
		@Column( name = "id_column" )
		private Long id;

		@Generated( event = EventType.INSERT )
		@ColumnDefault( "'default_name'" )
		private String name;

		@NaturalId
		private String data;

		public ValuesAndNaturalId() {
		}

		private ValuesAndNaturalId(Long id, String data) {
			this.id = id;
			this.data = data;
		}

		public String getName() {
			return name;
		}
	}
}
