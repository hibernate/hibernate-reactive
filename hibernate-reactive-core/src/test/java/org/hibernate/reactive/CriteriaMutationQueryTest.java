/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.query.criteria.JpaCriteriaInsertSelect;
import org.hibernate.query.criteria.JpaCriteriaInsertValues;
import org.hibernate.query.criteria.JpaCriteriaQuery;
import org.hibernate.query.criteria.JpaRoot;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.stage.Stage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.CriteriaUpdate;
import jakarta.persistence.criteria.Root;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class CriteriaMutationQueryTest extends BaseReactiveTest {
	private static final Integer SPELT_ID = 1;
	private static final String SPELT_NAME = "Spelt";
	private static final String SPELT_TYPE = "Wheat flour";
	Flour spelt = new Flour( SPELT_ID, SPELT_NAME, "An ancient grain, is a hexaploid species of wheat.", SPELT_TYPE );
	Flour rye = new Flour( 2, "Rye", "Used to bake the traditional sourdough breads of Germany.", "Wheat flour" );
	Flour almond = new Flour( 3, "Almond", "made from ground almonds.", "Gluten free" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Flour.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		test( context, getSessionFactory().withTransaction( s -> s.persist( spelt, rye, almond ) ) );
	}

	@Test
	public void testStageUpdateCriteriaQuery(VertxTestContext context) {
		String updatedDescription = "Most rye breads use a mix of rye and wheat flours";
		test( context, getSessionFactory()
				.withTransaction( s -> s
						.createMutationQuery( criteriaUpdate( getCriteriaBuilder( s ), updatedDescription, rye ) )
						.executeUpdate()
				)
				.thenAccept( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.thenCompose( v -> getSessionFactory()
						.withTransaction( s -> s.find( Flour.class, rye.getId() ) ) )
				.thenAccept( result -> assertThat( result.getDescription() ).isEqualTo( updatedDescription ) )
		);
	}

	@Test
	public void testMutinyUpdateCriteriaQuery(VertxTestContext context) {
		String updatedDescription = "made from ground almonds.";
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createMutationQuery( criteriaUpdate( getCriteriaBuilder( s ), updatedDescription, almond ) )
						.executeUpdate()
				)
				.invoke( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.chain( v -> getMutinySessionFactory()
						.withTransaction( s -> s.find( Flour.class, almond.getId() ) ) )
				.invoke( result -> assertThat( result.getDescription() ).isEqualTo( updatedDescription ) )
		);
	}

	@Test
	public void testStageDeleteCriteriaQuery(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( s -> s
						.createMutationQuery( criteriaUpdate( getCriteriaBuilder( s ) ) )
						.executeUpdate()
				)
				.thenAccept( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.thenCompose( v -> getSessionFactory()
						.withTransaction( s -> s.find( Flour.class, spelt.getId() ) ) )
				.thenAccept( result -> assertThat( result ).isNull() )
		);
	}

	@Test
	public void testMutinyDeleteCriteriaQuery(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> {
					return s.createMutationQuery( criteriaUpdate( getCriteriaBuilder( s ) ) ).executeUpdate();
				} )
				.invoke( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.chain( v -> getMutinySessionFactory()
						.withTransaction( s -> s.find( Flour.class, spelt.getId() ) ) )
				.invoke( result -> assertThat( result ).isNull() )
		);
	}

	@Test
	public void testStageInsertCriteriaQuery(VertxTestContext context) {
		final int id = 4;
		final String flourName = "Rye";
		final String flourDescription = "Used to bake the traditional sourdough breads of Germany.";
		final String flourType = "Wheat flour";
		test( context, getSessionFactory()
				.withTransaction( s -> s
						.createMutationQuery( insertCriteria( getCriteriaBuilder( s ), id, flourName, flourDescription, flourType ) )
						.executeUpdate()
				)
				.thenAccept( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.thenCompose( v -> getSessionFactory()
						.withTransaction( s -> s.find( Flour.class, id ) ) )
				.thenAccept( result -> {
					assertThat( result ).isNotNull();
					assertThat( result.name ).isEqualTo( flourName );
					assertThat( result.description ).isEqualTo( flourDescription );
					assertThat( result.type ).isEqualTo( flourType );
				} )
		);
	}

	@Test
	public void testMutinyInsertCriteriaQuery(VertxTestContext context) {
		final int id = 4;
		final String flourName = "Almond";
		final String flourDescription = "made from ground almonds.";
		final String flourType = "Gluten free";
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createMutationQuery( insertCriteria( getCriteriaBuilder( s ), id, flourName, flourDescription, flourType ) )
						.executeUpdate()
				)
				.invoke( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.chain( v -> getMutinySessionFactory()
						.withTransaction( s -> s.find( Flour.class, id ) ) )
				.invoke( result -> {
					assertThat( result ).isNotNull();
					assertThat( result.name ).isEqualTo( flourName );
					assertThat( result.description ).isEqualTo( flourDescription );
					assertThat( result.type ).isEqualTo( flourType );
				} )
		);
	}

	@Test
	public void testStageInsertSelectCriteriaQuery(VertxTestContext context) {
		final int idOfTheNewFlour = 4;
		test( context, getSessionFactory()
				.withTransaction( s -> s
						.createMutationQuery( insertSelectCriteria( getCriteriaBuilder( s ), idOfTheNewFlour ) )
						.executeUpdate()
				)
				.thenAccept( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.thenCompose( v -> getSessionFactory()
						.withTransaction( s -> s.find( Flour.class, idOfTheNewFlour ) ) )
				.thenAccept( result -> {
					assertThat( result ).isNotNull();
					assertThat( result.name ).isEqualTo( SPELT_NAME );
					assertThat( result.description ).isNull();
					assertThat( result.type ).isEqualTo( SPELT_TYPE );
				} )
		);
	}

	@Test
	public void testMutinyInsertSelectCriteriaQuery(VertxTestContext context) {
		final int idOfTheNewFlour = 4;
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s
						.createMutationQuery( insertSelectCriteria( getCriteriaBuilder( s ), idOfTheNewFlour ) )
						.executeUpdate()
				)
				.invoke( resultCount -> assertThat( resultCount ).isEqualTo( 1 ) )
				.chain( v -> getMutinySessionFactory()
						.withTransaction( s -> s.find( Flour.class, idOfTheNewFlour ) ) )
				.invoke( result -> {
					assertThat( result ).isNotNull();
					assertThat( result.name ).isEqualTo( SPELT_NAME );
					assertThat( result.description ).isNull();
					assertThat( result.type ).isEqualTo( SPELT_TYPE );
				} )
		);
	}

	private CriteriaUpdate<Flour> criteriaUpdate(CriteriaBuilder cb, String updatedDescription, Flour rye) {
		CriteriaUpdate<Flour> criteriaUpdate = cb.createCriteriaUpdate( Flour.class );
		Root<Flour> from = criteriaUpdate.from( Flour.class );
		criteriaUpdate.set( from.get( "description" ), updatedDescription );
		criteriaUpdate.where( cb.equal( from.get( "id" ), rye.getId() ) );
		return criteriaUpdate;
	}

	private CriteriaDelete<Flour> criteriaUpdate(CriteriaBuilder criteriaBuilder) {
		CriteriaDelete<Flour> criteriaDelete = criteriaBuilder.createCriteriaDelete( Flour.class );
		Root<Flour> from = criteriaDelete.from( Flour.class );
		criteriaDelete.where( criteriaBuilder.equal( from.get( "id" ), spelt.getId() ) );
		return criteriaDelete;
	}

	private static JpaCriteriaInsertValues<Flour> insertCriteria(HibernateCriteriaBuilder cb, int id, String name, String description, String type) {
		JpaCriteriaInsertValues<Flour> insert = cb.createCriteriaInsertValues( Flour.class );
		JpaRoot<Flour> flour = insert.getTarget();
		insert.setInsertionTargetPaths(	flour.get( "id" ), flour.get( "name" ), flour.get( "description" ), flour.get( "type" ) );
		insert.values( cb.values( cb.value( id ), cb.value( name ), cb.value( description ), cb.value( type ) ) );
		return insert;
	}

	private static JpaCriteriaInsertSelect<Flour> insertSelectCriteria(
			HibernateCriteriaBuilder cb,
			int idOfTheNewFlour) {
    	/*
		 The query executes and insert of Flour with id equals to 2 a name and type
		 selected from the existing spelt flour saved in the db
	 	*/
		JpaCriteriaInsertSelect<Flour> insertSelect = cb.createCriteriaInsertSelect( Flour.class );
		// columns to insert
		JpaRoot<Flour> flour = insertSelect.getTarget();
		insertSelect.setInsertionTargetPaths( flour.get( "id" ), flour.get( "name" ), flour.get( "type" ) );
		// select query
		JpaCriteriaQuery<Tuple> select = cb.createQuery( Tuple.class );
		JpaRoot<Flour> root = select.from( Flour.class );
		select.multiselect( cb.literal( idOfTheNewFlour ), root.get( "name" ), root.get( "type" )  );
		select.where( cb.equal( root.get( "id" ), SPELT_ID ) );

		insertSelect.select( select );
		return insertSelect;
	}

	private static HibernateCriteriaBuilder getCriteriaBuilder(Stage.Session session) {
		return session.getFactory().getCriteriaBuilder();
	}

	private static HibernateCriteriaBuilder getCriteriaBuilder(Mutiny.Session session) {
		return session.getFactory().getCriteriaBuilder();
	}

	@Entity(name = "Flour")
	@Table(name = "Flour")
	public static class Flour {
		@Id
		private Integer id;
		private String name;
		private String description;
		private String type;

		public Flour() {
		}

		public Flour(Integer id, String name, String description, String type) {
			this.id = id;
			this.name = name;
			this.description = description;
			this.type = type;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		@Override
		public String toString() {
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
			Flour flour = (Flour) o;
			return Objects.equals( name, flour.name ) &&
					Objects.equals( description, flour.description ) &&
					Objects.equals( type, flour.type );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name, description, type );
		}
	}
}
