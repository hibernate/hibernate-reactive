/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.TransientPropertyValueException;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;

import org.junit.After;
import org.junit.Test;

import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.unit.TestContext;

public class JoinColumnsTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( SampleEntity.class );
		configuration.addAnnotatedClass( SampleJoinEntity.class );
		return configuration;
	}

	@After
	public void cleanDB(TestContext context) {
		test( context, getMutinySessionFactory()
				.withSession( session -> session
						.createQuery( "delete SampleJoinEntity" ).executeUpdate()
						.invoke( ignore -> session
								.createQuery( "delete SampleEntity" ).executeUpdate() ) ) );
	}

	@Test
	public void testWithStages(TestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( sampleEntity ) )
				.thenCompose( ignore -> getSessionFactory()
						.withTransaction( (session, tx) -> session.merge( sampleEntity )
								.thenCompose( merged -> {
									sampleJoinEntity.sampleEntity = merged;
									merged.sampleJoinEntities.add( sampleJoinEntity );
									return session.persist( sampleJoinEntity );
								} )
						)
				)
				.thenCompose( ignore -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id )
								.thenAccept( entity -> context.assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testWithMutiny(TestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( sampleEntity ) )
				.then( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session.merge( sampleEntity )
								.invokeUni( merged -> {
									sampleJoinEntity.sampleEntity = merged;
									merged.sampleJoinEntities.add( sampleJoinEntity );
									return session.persist( sampleJoinEntity );
								} )
						)
				)
				.then( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id )
								.invoke( entity -> context.assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testDetachedReferenceWithStages(TestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getSessionFactory()
				.withTransaction( (session, transaction) -> session.persist( sampleEntity ) )
		 		.thenCompose( ignore -> getSessionFactory()
						.withTransaction( (session, tx) -> {
							sampleJoinEntity.sampleEntity = sampleEntity;
							sampleEntity.sampleJoinEntities.add( sampleJoinEntity );
							return session.persist( sampleJoinEntity );
						} )
				)
				.thenCompose( ignore -> getSessionFactory()
						.withTransaction( (session, tx) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id )
								.thenAccept( entity -> context.assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testDetachedReferenceWithMutiny(TestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( sampleEntity ) )
				.then( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> {
							sampleJoinEntity.sampleEntity = sampleEntity;
							sampleEntity.sampleJoinEntities.add( sampleJoinEntity );
							return session.persist( sampleJoinEntity );
						} )
				)
				.then( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id )
								.invoke( entity -> context.assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testTransientReferenceExceptionWithStages(TestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getSessionFactory()
				.withTransaction( (session, tx) -> {
							sampleJoinEntity.sampleEntity = sampleEntity;
							sampleEntity.sampleJoinEntities.add( sampleJoinEntity );
							return session.persist( sampleJoinEntity );
						} )
				.handle( (session, throwable) -> {
					context.assertEquals( CompletionException.class, throwable.getClass() );
					context.assertEquals( IllegalStateException.class, throwable.getCause().getClass() );
					context.assertEquals( TransientPropertyValueException.class, throwable.getCause().getCause().getClass() );
					return null;
				} )
		);
	}

	@Test
	public void testTransientReferenceExceptionWithMutiny(TestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getMutinySessionFactory().withTransaction( (session, tx) -> {
							sampleJoinEntity.sampleEntity = sampleEntity;
							sampleEntity.sampleJoinEntities.add( sampleJoinEntity );
							return session.persist( sampleJoinEntity );
						} )
				.onItem().invoke( session -> context.fail( "Expected exception not thrown" ) )
				.onFailure().recoverWithUni( throwable -> {
					context.assertNotNull( throwable );
					context.assertEquals( CompositeException.class, throwable.getClass() );
					context.assertEquals( IllegalStateException.class, throwable.getCause().getClass() );
					context.assertEquals( TransientPropertyValueException.class, throwable.getCause().getCause().getClass() );
					return Uni.createFrom().nullItem();
				} )
		);
	}

	@Entity(name = "SampleEntity")
	@Table(name = "sample_entities")
	public static class SampleEntity implements Serializable {
		@Id
		@Column(name = "first_key_id", nullable = false)
		public Long firstKeyId;

		@Id
		@Column(name = "second_key_id", nullable = false)
		public Long secondKeyId;

		public String name;

		@OneToMany(fetch = FetchType.LAZY, mappedBy = "sampleEntity")
		public Set<SampleJoinEntity> sampleJoinEntities = new HashSet<>();
	}

	@Entity(name = "SampleJoinEntity")
	@Table(name = "sample_join_entities")
	public static class SampleJoinEntity implements Serializable {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Long id;

		public String name;
		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumns({
				@JoinColumn(name = "first_key_id", referencedColumnName = "first_key_id"),
				@JoinColumn(name = "second_key_id", referencedColumnName = "second_key_id")
		})
		public SampleEntity sampleEntity;
	}
}
