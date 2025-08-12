/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.annotations.ColumnDefault;
import org.hibernate.annotations.Generated;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.RowId;
import org.hibernate.annotations.SourceType;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.generator.EventType;
import org.hibernate.generator.values.GeneratedValuesMutationDelegate;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.mutiny.impl.MutinySessionImpl;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.sql.model.MutationType;

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
public class MutationDelegateTest extends BaseReactiveTest{

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ValuesOnly.class,ValuesAndRowId.class, ValuesAndNaturalId.class );
	}

	@Test
	public void testInsertGeneratedValues(VertxTestContext context) {
		ValuesOnly entity = new ValuesOnly( 1L );
		test(
				context, getMutinySessionFactory().withTransaction(
						s -> s.persist( entity ).call( s::flush )
								.invoke( () -> assertThat( entity.getName() ).isEqualTo( "default_name" ) )
				)
		);
	}

	@Test
	public void testUpdateGeneratedValues(VertxTestContext context) {
		final ValuesOnly entity = new ValuesOnly( 2L );
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( entity ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( ValuesOnly.class, 2 )
						.invoke( valuesOnly -> valuesOnly.setData( "changed" )  )
				 ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( ValuesOnly.class, 2 ) )
						.invoke( valuesOnly -> assertThat( entity.getUpdateDate() ).isNotNull()
						)
				)
		);
	}

	@Test
	@DisabledFor(value = ORACLE, reason = "Vert.x driver doesn't support RowId type parameters")
	public void testGeneratedValuesAndRowId(VertxTestContext context) {
		final GeneratedValuesMutationDelegate delegate = getDelegate(
				ValuesAndRowId.class,
				MutationType.INSERT
		);
		final boolean shouldHaveRowId = delegate != null && delegate.supportsRowId()
				&& getDialect().rowId( "" ) != null;

		final ValuesAndRowId entity = new ValuesAndRowId( 1L );
		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> {
					assertThat( entity.getName() ).isEqualTo( "default_name" );
					s.getFactory();
					if ( shouldHaveRowId ) {
						// assert row-id was populated in entity entry
						final PersistenceContext pc = ( (MutinySessionImpl) s ).unwrap(	ReactiveSession.class )
								.getPersistenceContext();
						final EntityEntry entry = pc.getEntry( entity );
						assertThat( entry.getRowId() ).isNotNull();
					}
					entity.setData( "changed" );
				} ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
				.find( ValuesAndRowId.class, 1 )
				.invoke( valuesAndRowId -> assertThat( valuesAndRowId.getUpdateDate() ).isNotNull() )
			  ) )
		);
	}

	@Test
	public void testInsertGeneratedValuesAndNaturalId(VertxTestContext context) {
		final ValuesAndNaturalId entity = new ValuesAndNaturalId( 1L, "natural_1" );

		test( context, getMutinySessionFactory().withTransaction( s -> s
				.persist( entity )
				.call( s::flush )
				.invoke( () -> assertThat( entity.getName() ).isEqualTo( "default_name" ) )
			  )

		);
	}

	private static GeneratedValuesMutationDelegate getDelegate(Class<?> entityClass, MutationType mutationType) {
		return ( (SessionFactoryImplementor) factoryManager
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
