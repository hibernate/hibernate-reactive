/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.HibernateException;
import org.hibernate.LazyInitializationException;
import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;

/**
 * Test the stateless update of a proxy.
 * <p>
 *     Note that it's required to update and read the values from the proxy using getter/setter and
 *     there is no guarantee about this working otherwise.
 * </p>
 *
 * @see org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl#reactiveUpdate(Object)
 * @see ReactiveStatelessSessionTest
 */
public class ReactiveStatelessProxyUpdateTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( SampleEntity.class );
		configuration.addAnnotatedClass( SampleJoinEntity.class );
		return configuration;
	}

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "SampleEntity", "SampleJoinEntity" ) );
	}

	@Test
	public void testUnfetchedEntityException(TestContext context) {
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.sampleField = "test";

		SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.sampleEntity = sampleEntity;

		test( context, assertThrown( HibernateException.class, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persistAll( sampleEntity, sampleJoinEntity ) )
				.chain( targetId -> getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.getId() ) )
				)
				.chain( joinEntityFromDatabase -> getMutinySessionFactory().withStatelessTransaction(
						(s, t) -> {
							SampleEntity entityFromDb = joinEntityFromDatabase.sampleEntity;
							entityFromDb.sampleField = "updated field";
							// We expect the update to fail because we haven't fetched the entity
							// and therefore the proxy is uninitialized
							return s.update( entityFromDb );
						} )
				) )
				.invoke( exception -> assertThat( exception.getMessage() ).contains( "HR000072" ) )
		);
	}

	@Test
	public void testLazyInitializationException(TestContext context) {
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.sampleField = "test";

		SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.sampleEntity = sampleEntity;

		test( context, assertThrown( LazyInitializationException.class, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persistAll( sampleEntity, sampleJoinEntity ) )

				.chain( targetId -> getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.getId() ) )
				)

				.chain( joinEntityFromDatabase -> getMutinySessionFactory().withStatelessTransaction(
						(s, t) -> {
							// LazyInitializationException here because we haven't fetched the entity
							SampleEntity entityFromDb = joinEntityFromDatabase.getSampleEntity();
							entityFromDb.setSampleField( "updated field" );
							return s.update( entityFromDb );
						} )
				) )
		);
	}

	@Test
	public void testUpdateWithInitializedProxy(TestContext context) {
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.setSampleField( "test" );

		SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.setSampleEntity( sampleEntity );

		test( context, getMutinySessionFactory()
					  .withTransaction( (s, t) -> s.persistAll( sampleEntity, sampleJoinEntity ) )

					  .chain( targetId -> getMutinySessionFactory()
							  .withTransaction( (session, transaction) -> session
									  .find( SampleJoinEntity.class, sampleJoinEntity.getId() ) )
					  )

					  .chain( joinEntityFromDatabase -> getMutinySessionFactory().withStatelessTransaction(
							  (s, t) -> s
									  // The update of the associated entity should work if we fetch it first
									  .fetch( joinEntityFromDatabase.getSampleEntity() )
									  .chain( fetchedEntity -> {
										  fetchedEntity.setSampleField( "updated field" );
										  return s.update( fetchedEntity );
									  } ) )
					  )
					  .chain( () -> getMutinySessionFactory().withSession( session -> session
							  .createQuery( "from SampleEntity", SampleEntity.class )
							  .getSingleResult()
							  .onItem().invoke( result -> context.assertEquals( "updated field", result.getSampleField() ) ) )
					  )
		);
	}

	@Entity(name = "SampleEntity")
	@Table(name = "sample_entities")
	public static class SampleEntity implements Serializable {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		@Column(name = "sample_field")
		private String sampleField;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getSampleField() {
			return sampleField;
		}

		public void setSampleField(String sampleField) {
			this.sampleField = sampleField;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + " ID: " + id + " sampleField: " + sampleField;
		}
	}

	@Entity(name = "SampleJoinEntity")
	@Table(name = "sample_join_entities")
	public static class SampleJoinEntity implements Serializable {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private Long id;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "sample_entity_id", referencedColumnName = "id")
		private SampleEntity sampleEntity;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public SampleEntity getSampleEntity() {
			return sampleEntity;
		}

		public void setSampleEntity(SampleEntity sampleEntity) {
			this.sampleEntity = sampleEntity;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + " ID: " + id + " sampleEntity: " + sampleEntity;
		}
	}
}
