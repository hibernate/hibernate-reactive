/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;

import org.hibernate.TransientPropertyValueException;
import org.hibernate.reactive.BaseReactiveTest;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class JoinColumnsTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SampleJoinEntity.class, SampleEntity.class );
	}

	@Test
	public void testWithStages(VertxTestContext context) {
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
								.thenAccept( entity -> assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testWithMutiny(VertxTestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( sampleEntity ) )
				.call( () -> getMutinySessionFactory().withTransaction( (session, tx) -> session
							   .merge( sampleEntity )
							   .chain( merged -> {
								   sampleJoinEntity.sampleEntity = merged;
								   merged.sampleJoinEntities.add( sampleJoinEntity );
								   return session.persist( sampleJoinEntity );
							   } )
					   )
				)
				.call( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id )
								.invoke( entity -> assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testDetachedReferenceWithStages(VertxTestContext context) {
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
								.thenAccept( entity -> assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testDetachedReferenceWithMutiny(VertxTestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getMutinySessionFactory()
				.withTransaction( (session, transaction) -> session.persist( sampleEntity ) )
				.call( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> {
							sampleJoinEntity.sampleEntity = sampleEntity;
							sampleEntity.sampleJoinEntities.add( sampleJoinEntity );
							return session.persist( sampleJoinEntity );
						} )
				)
				.call( () -> getMutinySessionFactory()
						.withTransaction( (session, tx) -> session
								.find( SampleJoinEntity.class, sampleJoinEntity.id )
								.invoke( entity -> assertEquals( sampleJoinEntity.name, entity.name ) )
						)
				)
		);
	}

	@Test
	public void testTransientReferenceExceptionWithStages(VertxTestContext context) {
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
					assertNotNull( throwable );
					assertEquals( CompletionException.class, throwable.getClass() );
					assertEquals( IllegalStateException.class, throwable.getCause().getClass() );
					assertEquals( TransientPropertyValueException.class, throwable.getCause().getCause().getClass() );
					return null;
				} )
		);
	}

	@Test
	public void testTransientReferenceExceptionWithMutiny(VertxTestContext context) {
		final SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.name = "Entity name";
		sampleEntity.firstKeyId = 1L;
		sampleEntity.secondKeyId = 2L;

		final SampleJoinEntity sampleJoinEntity = new SampleJoinEntity();
		sampleJoinEntity.name = "Joined entity name";

		test( context, getMutinySessionFactory()
				.withTransaction( (session, tx) -> {
					sampleJoinEntity.sampleEntity = sampleEntity;
					sampleEntity.sampleJoinEntities.add( sampleJoinEntity );
					return session.persist( sampleJoinEntity );
				} )
				.onItem().invoke( v -> context.failNow( "Expected exception not thrown" ) )
				.onFailure().recoverWithUni( throwable -> {
					assertNotNull( throwable );
					assertEquals( IllegalStateException.class, throwable.getClass() );
					assertEquals( TransientPropertyValueException.class, throwable.getCause().getClass() );
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
