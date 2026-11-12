/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.CustomEntityDirtinessStrategy;
import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.hibernate.persister.entity.EntityPersister;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Access;
import jakarta.persistence.AccessType;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.cfg.AvailableSettings.CUSTOM_ENTITY_DIRTINESS_STRATEGY;

/**
 * Test that the {@link CustomEntityDirtinessStrategy} resets the dirty state
 * of an entity after it's been loaded, like Hibernate ORM does.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class CustomEntityDirtinessStrategyTest extends BaseReactiveTest {

	private final Book book = new Book( 1, "The Hydrogen Sonata" );

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		final Configuration configuration = super.constructConfiguration();
		configuration.setProperty( CUSTOM_ENTITY_DIRTINESS_STRATEGY, DirtyTrackerStrategy.class.getName() );
		return configuration;
	}

	@Test
	public void loadedEntityIsNotDirty(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persist( book ) )
				.map( v -> book.getVersion() )
				.call( versionAfterPersist -> getMutinySessionFactory()
						.withTransaction( s -> s.find( Book.class, book.getId() ) )
						.invoke( found -> assertThat( found ).isEqualTo( book ) ) )
				.call( versionAfterPersist -> getMutinySessionFactory()
						.withSession( s -> s.find( Book.class, book.getId() ) )
						// Loading the entity in a transaction must not cause an update
						.invoke( found -> assertThat( found.getVersion() ).isEqualTo( versionAfterPersist ) ) )
		);
	}

	public interface DirtyTracker {
		boolean isDirty();

		void resetDirty();
	}

	public static class DirtyTrackerStrategy implements CustomEntityDirtinessStrategy {
		@Override
		public boolean canDirtyCheck(Object entity, EntityPersister persister, Session session) {
			return entity instanceof DirtyTracker;
		}

		@Override
		public boolean isDirty(Object entity, EntityPersister persister, Session session) {
			return ( (DirtyTracker) entity ).isDirty();
		}

		@Override
		public void resetDirty(Object entity, EntityPersister persister, Session session) {
			( (DirtyTracker) entity ).resetDirty();
		}

		@Override
		public void findDirty(Object entity, EntityPersister persister, Session session, DirtyCheckContext dirtyCheckContext) {
			final boolean dirty = ( (DirtyTracker) entity ).isDirty();
			dirtyCheckContext.doDirtyChecking( attributeInformation -> dirty );
		}
	}

	@Entity(name = "Book")
	@Table(name = "CEDSBook")
	@Access(AccessType.PROPERTY)
	public static class Book implements DirtyTracker {
		private Integer id;
		private String title;
		private int version;
		private boolean dirty;

		public Book() {
		}

		public Book(Integer id, String title) {
			this.id = id;
			this.title = title;
		}

		@Id
		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
			this.dirty = true;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
			this.dirty = true;
		}

		@Version
		public int getVersion() {
			return version;
		}

		public void setVersion(int version) {
			this.version = version;
		}

		@Override
		@Transient
		public boolean isDirty() {
			return dirty;
		}

		@Override
		public void resetDirty() {
			dirty = false;
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof Book other
					&& Objects.equals( title, other.getTitle() );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( title );
		}
	}
}
