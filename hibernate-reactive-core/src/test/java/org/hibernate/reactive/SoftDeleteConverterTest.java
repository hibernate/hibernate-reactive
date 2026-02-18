/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.annotations.SoftDelete;
import org.hibernate.annotations.SoftDeleteType;
import org.hibernate.type.TrueFalseConverter;
import org.hibernate.type.YesNoConverter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

/**
 * Tests @SoftDelete annotation with different converter types and strategies.
 */
public class SoftDeleteConverterTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of(
				YesNoEntity.class,
				TrueFalseEntity.class,
				ActiveStrategyEntity.class,
				DeletedStrategyEntity.class,
				DefaultEntity.class
		);
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> {
					YesNoEntity yn1 = new YesNoEntity( 1, "YesNo 1" );
					YesNoEntity yn2 = new YesNoEntity( 2, "YesNo 2" );

					TrueFalseEntity tf1 = new TrueFalseEntity( 1, "TrueFalse 1" );
					TrueFalseEntity tf2 = new TrueFalseEntity( 2, "TrueFalse 2" );

					ActiveStrategyEntity active1 = new ActiveStrategyEntity( 1, "Active 1" );
					ActiveStrategyEntity active2 = new ActiveStrategyEntity( 2, "Active 2" );

					DeletedStrategyEntity deleted1 = new DeletedStrategyEntity( 1, "Deleted 1" );
					DeletedStrategyEntity deleted2 = new DeletedStrategyEntity( 2, "Deleted 2" );

					DefaultEntity default1 = new DefaultEntity( 1, "Default 1" );
					DefaultEntity default2 = new DefaultEntity( 2, "Default 2" );

					return session.persistAll( yn1, yn2, tf1, tf2, active1, active2, deleted1, deleted2, default1, default2 );
				} )
		);
	}

	// Entities are annotated with @SoftDelete, we need to execute a native query to actually empty the table
	@Override
	protected CompletionStage<Void> cleanDb() {
		return loop(
				List.of( "YesNoEntity", "TrueFalseEntity", "ActiveStrategyEntity", "DeletedStrategyEntity", "DefaultEntity" ),
				tableName -> getSessionFactory().withTransaction( s -> s.createNativeQuery( "delete from " + tableName ).executeUpdate() )
		);
	}

	@Test
	public void testYesNoConverter(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete one entity
				.withTransaction( s -> s
						.find( YesNoEntity.class, 1 )
						.chain( s::remove )
				)
				// Verify it's not found
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( YesNoEntity.class, 1 )
						.invoke( entity -> assertThat( entity ).isNull() )
				) )
				// Check native query - should have 'Y' in deleted column
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, name, deleted from YesNoEntity order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							Object[] firstRow = (Object[]) rows.get( 0 );
							assertThat( firstRow[2] ).isEqualTo( "Y" );

							Object[] secondRow = (Object[]) rows.get( 1 );
							assertThat( secondRow[2] ).isEqualTo( "N" );
						} )
				) )
		);
	}

	@Test
	public void testTrueFalseConverter(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete one entity
				.withTransaction( s -> s
						.find( TrueFalseEntity.class, 1 )
						.chain( s::remove )
				)
				// Verify it's not found
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( TrueFalseEntity.class, 1 )
						.invoke( entity -> assertThat( entity ).isNull() )
				) )
				// Check native query - should have 'T' in deleted column
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, name, deleted from TrueFalseEntity order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							Object[] firstRow = (Object[]) rows.get( 0 );
							assertThat( firstRow[2] ).isEqualTo( "T" );

							Object[] secondRow = (Object[]) rows.get( 1 );
							assertThat( secondRow[2] ).isEqualTo( "F" );
						} )
				) )
		);
	}

	@Test
	public void testActiveStrategy(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete one entity
				.withTransaction( s -> s
						.find( ActiveStrategyEntity.class, 1 )
						.chain( s::remove )
				)
				// Verify it's not found
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( ActiveStrategyEntity.class, 1 )
						.invoke( entity -> assertThat( entity ).isNull() )
				) )
				// Check native query - should have 'N' (not active) in active column
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, name, active from ActiveStrategyEntity order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							Object[] firstRow = (Object[]) rows.get( 0 );
							assertThat( firstRow[2] ).isEqualTo( "N" ); // Not active

							Object[] secondRow = (Object[]) rows.get( 1 );
							assertThat( secondRow[2] ).isEqualTo( "Y" ); // Active
						} )
				) )
		);
	}

	@Test
	public void testDeletedStrategy(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete one entity
				.withTransaction( s -> s
						.find( DeletedStrategyEntity.class, 1 )
						.chain( s::remove )
				)
				// Verify it's not found
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( DeletedStrategyEntity.class, 1 )
						.invoke( entity -> assertThat( entity ).isNull() )
				) )
				// Check native query - should have 'Y' (deleted) in deleted column
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, name, deleted from DeletedStrategyEntity order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							Object[] firstRow = (Object[]) rows.get( 0 );
							assertThat( firstRow[2] ).isEqualTo( "Y" ); // Deleted

							Object[] secondRow = (Object[]) rows.get( 1 );
							assertThat( secondRow[2] ).isEqualTo( "N" ); // Not deleted
						} )
				) )
		);
	}

	@Test
	public void testDefaultConverter(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				// Delete one entity
				.withTransaction( s -> s
						.find( DefaultEntity.class, 1 )
						.chain( s::remove )
				)
				// Verify it's not found
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( DefaultEntity.class, 1 )
						.invoke( entity -> assertThat( entity ).isNull() )
				) )
				// Check native query - default uses boolean/numeric
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, name, deleted from DefaultEntity order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							Object[] firstRow = (Object[]) rows.get( 0 );
							// DB2 uses smallint, others use boolean
							if ( dbType() == DB2 ) {
								assertThat( (short) firstRow[2] ).isEqualTo( (short) 1 );
							}
							else {
								assertThat( (boolean) firstRow[2] ).isTrue();
							}

							Object[] secondRow = (Object[]) rows.get( 1 );
							if ( dbType() == DB2 ) {
								assertThat( (short) secondRow[2] ).isEqualTo( (short) 0 );
							}
							else {
								assertThat( (boolean) secondRow[2] ).isFalse();
							}
						} )
				) )
		);
	}

	@Entity(name = "YesNoEntity")
	@Table(name = "YesNoEntity")
	@SoftDelete(converter = YesNoConverter.class)
	public static class YesNoEntity {
		@Id
		private Integer id;
		private String name;

		public YesNoEntity() {
		}

		public YesNoEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			YesNoEntity that = (YesNoEntity) o;
			return Objects.equals( id, that.id );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id );
		}
	}

	@Entity(name = "TrueFalseEntity")
	@Table(name = "TrueFalseEntity")
	@SoftDelete(converter = TrueFalseConverter.class)
	public static class TrueFalseEntity {
		@Id
		private Integer id;
		private String name;

		public TrueFalseEntity() {
		}

		public TrueFalseEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			TrueFalseEntity that = (TrueFalseEntity) o;
			return Objects.equals( id, that.id );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id );
		}
	}

	@Entity(name = "ActiveStrategyEntity")
	@Table(name = "ActiveStrategyEntity")
	@SoftDelete(converter = YesNoConverter.class, strategy = SoftDeleteType.ACTIVE)
	public static class ActiveStrategyEntity {
		@Id
		private Integer id;
		private String name;

		public ActiveStrategyEntity() {
		}

		public ActiveStrategyEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			ActiveStrategyEntity that = (ActiveStrategyEntity) o;
			return Objects.equals( id, that.id );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id );
		}
	}

	@Entity(name = "DeletedStrategyEntity")
	@Table(name = "DeletedStrategyEntity")
	@SoftDelete(converter = YesNoConverter.class, strategy = SoftDeleteType.DELETED)
	public static class DeletedStrategyEntity {
		@Id
		private Integer id;
		private String name;

		public DeletedStrategyEntity() {
		}

		public DeletedStrategyEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			DeletedStrategyEntity that = (DeletedStrategyEntity) o;
			return Objects.equals( id, that.id );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id );
		}
	}

	@Entity(name = "DefaultEntity")
	@Table(name = "DefaultEntity")
	@SoftDelete
	public static class DefaultEntity {
		@Id
		private Integer id;
		private String name;

		public DefaultEntity() {
		}

		public DefaultEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			DefaultEntity that = (DefaultEntity) o;
			return Objects.equals( id, that.id );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id );
		}
	}
}
