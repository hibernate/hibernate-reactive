/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.SourceType;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.id.insert.AbstractSelectingDelegate;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.sql.model.MutationType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Inspired by the test
 * {@code org.hibernate.orm.test.mapping.generated.delegate.MutationDelegateJoinedInheritanceTest}
 * in Hibernate ORM.
 */
public class MutationDelegateJoinedInheritanceTest extends BaseReactiveTest {
	private static SqlStatementTracker sqlTracker;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( BaseEntity.class, ChildEntity.class, NonGeneratedParent.class, GeneratedChild.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();

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
	public void testInsertBaseEntity(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( BaseEntity.class, MutationType.INSERT );
		final int expectedQueriesSize = delegate instanceof AbstractSelectingDelegate
				? 3
				: delegate != null && delegate.supportsArbitraryValues() ? 1 : 2;
		final BaseEntity entity = new BaseEntity();

		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> {
					assertThat( entity.getId() ).isNotNull();
					assertThat( entity.getName() ).isEqualTo( "default_name" );

					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
				} ) )
		);
	}

	@Test
	public void testInsertChildEntity(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( ChildEntity.class, MutationType.INSERT );
		ChildEntity entity = new ChildEntity();
		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> {
					assertThat( entity.getId() ).isNotNull();
					assertThat( entity.getName() ).isEqualTo( "default_name" );
					assertThat( entity.getChildName() ).isEqualTo( "default_child_name" );

					if ( delegate instanceof AbstractSelectingDelegate ) {
						assertThat( sqlTracker.getLoggedQueries() ).hasSize( 4 );
						assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
						assertThat( sqlTracker.getLoggedQueries().get( 2 ) ).contains( "insert" );
						// Note: this is a current restriction, mutation delegates only retrieve generated values
						// on the "root" table, and we expect other values to be read through a subsequent select
						assertThat( sqlTracker.getLoggedQueries().get( 3 ) ).startsWith( "select" );

					}
					else {
						assertThat( sqlTracker.getLoggedQueries() ).hasSize( 3 );
						assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).contains( "insert" );
						assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).contains( "insert" );
						// Note: this is a current restriction, mutation delegates only retrieve generated values
						// on the "root" table, and we expect other values to be read through a subsequent select
						assertThat( sqlTracker.getLoggedQueries().get( 2 ) ).startsWith( "select" );
					}
				} ) )
		);
	}

	@Test
	public void testUpdateBaseEntity(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( BaseEntity.class, MutationType.UPDATE );
		final int expectedQueriesSize = delegate != null && delegate.supportsArbitraryValues() ? 2 : 3;
		final BaseEntity entity = new BaseEntity();

		test( context, getMutinySessionFactory().withTransaction( s -> s.persist( entity ) )
				.invoke( () -> sqlTracker.clear() )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
								.find( BaseEntity.class, entity.getId() )
								.invoke( baseEntity -> baseEntity.setData( "changed" ) )
								.call( s::flush )
								.invoke( baseEntity -> {
									assertThat( entity.getUpdateDate() ).isNotNull();
									assertThat( sqlTracker.getLoggedQueries() ).hasSize( expectedQueriesSize );
									assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "select" );
									assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).contains( "update " );
								} )
						)
				)
		);
	}

	@Test
	public void testUpdateChildEntity(VertxTestContext context) {
		final ChildEntity entity = new ChildEntity();
		test( context, getMutinySessionFactory().withTransaction( s -> s.persist( entity ) )
				.invoke( () -> sqlTracker.clear() )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( ChildEntity.class, entity.getId() )
						.invoke( childEntity -> childEntity.setData( "changed" ) )
						.call( s::flush )
						.invoke( childEntity -> {
							assertThat( entity.getUpdateDate() ).isNotNull();
							assertThat( entity.getChildUpdateDate() ).isNotNull();

							assertThat( sqlTracker.getLoggedQueries() ).hasSize( 3 );
							assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "select" );
							assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).contains( "update " );
							// Note: this is a current restriction, mutation delegates only retrieve generated values
							// on the "root" table, and we expect other values to be read through a subsequent select
							assertThat( sqlTracker.getLoggedQueries().get( 2 ) ).startsWith( "select" );
						} )
				) )
		);
	}

	@Test
	public void testGeneratedOnlyOnChild(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate( NonGeneratedParent.class, MutationType.UPDATE );
		// Mutation delegates only support generated values on the "root" table
		assertThat( delegate ).isNull();
		final GeneratedChild generatedChild = new GeneratedChild();
		generatedChild.setId( 1L );
		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist(  generatedChild )
				.call( s::flush )
				.invoke( () -> {
					assertThat( generatedChild.getName() ).isEqualTo( "child_name" );
					assertThat( sqlTracker.getLoggedQueries() ).hasSize( 3 );
					assertThat( sqlTracker.getLoggedQueries().get( 0 ) ).startsWith( "insert" );
					assertThat( sqlTracker.getLoggedQueries().get( 1 ) ).startsWith( "insert" );
					assertThat( sqlTracker.getLoggedQueries().get( 2 ) ).startsWith( "select" );
				} )
		));
	}

	private static GeneratedValuesMutationDelegate getDelegate(Class<?> entityClass, MutationType mutationType) {
		return ( (SessionFactoryImplementor) factoryManager
				.getHibernateSessionFactory() )
				.getMappingMetamodel()
				.findEntityDescriptor( entityClass )
				.getMutationDelegate( mutationType );
	}

	@Entity(name = "BaseEntity")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static class BaseEntity {
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

	@Entity(name = "ChildEntity")
	public static class ChildEntity extends BaseEntity {
		@Generated(event = EventType.INSERT)
		@ColumnDefault("'default_child_name'")
		private String childName;

		@UpdateTimestamp(source = SourceType.DB)
		private Date childUpdateDate;

		public String getChildName() {
			return childName;
		}

		public Date getChildUpdateDate() {
			return childUpdateDate;
		}
	}

	@Entity(name = "NonGeneratedParent")
	@Inheritance(strategy = InheritanceType.JOINED)
	public static class NonGeneratedParent {
		@Id
		private Long id;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}
	}

	@Entity(name = "GeneratedChild")
	public static class GeneratedChild extends NonGeneratedParent {
		@Generated(event = EventType.INSERT)
		@ColumnDefault("'child_name'")
		private String name;

		public String getName() {
			return name;
		}
	}
}
