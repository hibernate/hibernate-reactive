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

import org.hibernate.cfg.Configuration;

import org.junit.After;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

/**
 * @see org.hibernate.reactive.session.impl.ReactiveStatelessSessionImpl#reactiveUpdate(Object)
 */
public class ReactiveStatelessUpdateTest  extends BaseReactiveTest {

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
	public void testUpdateWithDetachedEntity(TestContext context) {
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.sampleField = "test";

		SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.sampleEntity = sampleEntity;

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persistAll( sampleEntity, sampleJoinEntity ) )

				.chain( targetId -> getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id ) )
				)

				.chain( joinEntityFromDatabase -> getMutinySessionFactory().withStatelessTransaction(
						(s, t) -> {
							SampleEntity entityFromDb = joinEntityFromDatabase.sampleEntity;
							entityFromDb.sampleField = "updated field";
							// Intended to test updating unmanaged proxy entity in this transaction
							return s.update( entityFromDb );
						} )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.createQuery( "from SampleEntity", SampleEntity.class )
						.getSingleResult()
						.onItem().invoke( result -> context.assertEquals( "updated field", result.sampleField ) ) )
				)
		);
	}

	@Test
	public void testUpdateWithManagedProxyEntity(TestContext context) {
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.sampleField = "test";

		SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.sampleEntity = sampleEntity;

		test( context, getMutinySessionFactory()
				.withTransaction( (s, t) -> s.persistAll( sampleEntity, sampleJoinEntity ) )

				.chain( targetId -> getMutinySessionFactory()
						.withTransaction( (session, transaction) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id ) )
				)

				.chain( joinEntityFromDatabase -> getMutinySessionFactory().withStatelessTransaction(
						(s, t) -> s
								// fetching entity in this transaction to trigger logic for
								// updating a managed proxy entity
								.fetch( joinEntityFromDatabase.sampleEntity )
								.chain( fetchedEntity -> {
									SampleEntity entityFromDb = fetchedEntity;
									entityFromDb.sampleField = "updated field";
									return s.update( entityFromDb );
								} ) )
				)
				.chain( () -> getMutinySessionFactory().withSession( session -> session
						.createQuery( "from SampleEntity", SampleEntity.class )
						.getSingleResult()
						.onItem().invoke( result -> context.assertEquals("updated field", result.sampleField  ) ))
				)
		);
	}

	@Entity(name = "SampleEntity")
	@Table(name = "sample_entities")
	public static class SampleEntity implements Serializable {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Long id;

		@Column(name = "sample_field")
		public String sampleField;

		@Override
		public String toString() {
			return getClass().getSimpleName() + " ID: " + id + " sampleField: " + sampleField;
		}

		public String getSampleField() {
			return sampleField;
		}
	}

	@Entity(name = "SampleJoinEntity")
	@Table(name = "sample_join_entities")
	public static class SampleJoinEntity implements Serializable {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Long id;

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "sample_entity_id", referencedColumnName = "id")
		public SampleEntity sampleEntity;

		@Override
		public String toString() {
			return getClass().getSimpleName() + " ID: " + id + " sampleEntity: " + sampleEntity;
		}
	}
}
