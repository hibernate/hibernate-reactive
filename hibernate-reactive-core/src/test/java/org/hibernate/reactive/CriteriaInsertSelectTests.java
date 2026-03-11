/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.query.sqm.internal.SqmCriteriaNodeBuilder;
import org.hibernate.query.sqm.tree.from.SqmRoot;
import org.hibernate.query.sqm.tree.insert.SqmInsertSelectStatement;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.reactive.annotations.DisabledFor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Tuple;
import java.util.Collection;
import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;

@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor( value = MYSQL, reason = "INSERT-SELECT operations require bulk insertion capable identifiers and Mysql used a EmulatedSequenceReactiveIdentifierGenerator that is not compatible with bulk insertions" )
public class CriteriaInsertSelectTests extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( AnEntity.class );
	}

	@BeforeEach
	public void populateDb(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> session
				.persist( new AnEntity( "test" ) ) )
		);
	}

	@Test
	public void simpleTest(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction(
				session -> {
					final SqmCriteriaNodeBuilder criteriaBuilder = (SqmCriteriaNodeBuilder) session.getCriteriaBuilder();
					final SqmInsertSelectStatement<AnEntity> insertSelect = criteriaBuilder
							.createCriteriaInsertSelect( AnEntity.class );
					final SqmSelectStatement<Tuple> select = criteriaBuilder.createQuery( Tuple.class );
					insertSelect.addInsertTargetStateField( insertSelect.getTarget().get( "name" ) );
					final SqmRoot<AnEntity> root = select.from( AnEntity.class );
					select.select( root.get( "name" ) );
					insertSelect.setSelectQueryPart( select.getQuerySpec() );
					return session.createMutationQuery( insertSelect ).executeUpdate();
				} ).chain( () -> getMutinySessionFactory().withTransaction(
						session -> session.createQuery( "from AnEntity", AnEntity.class ).getResultCount()
								.invoke( aLong -> assertThat( aLong ).isEqualTo( 2L )) ) )
		);
	}

	@Entity(name = "AnEntity")
	public static class AnEntity {
		@Id
		@GeneratedValue
		private Integer id;
		@Basic
		private String name;

		private AnEntity() {
			// for use by Hibernate
		}

		public AnEntity(String name) {
			this.name = name;
		}

		public AnEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
