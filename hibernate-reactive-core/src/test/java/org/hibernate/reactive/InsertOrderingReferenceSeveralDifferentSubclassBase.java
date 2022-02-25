/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.List;
import jakarta.persistence.CascadeType;
import jakarta.persistence.DiscriminatorColumn;
import jakarta.persistence.DiscriminatorValue;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Inheritance;
import jakarta.persistence.InheritanceType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * Test ORDER_INSERTS property value during batching.
 *
 * Verify that duplicate sql insert statements are collapsed to a single insert per entity and are ordered based on
 * parent/child relationships`
 */
public abstract class InsertOrderingReferenceSeveralDifferentSubclassBase extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule rule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	public static class OrderedTest extends InsertOrderingReferenceSeveralDifferentSubclassBase {

		@Override
		public boolean isOrderInserts() {
			return true;
		}

		@Override
		public String[] getExpectedNativeQueries() {
			return new String[] {
					"insert into UnrelatedEntity (unrelatedValue, id) values ($1, $2)",
					"insert into BaseEntity (name, TYPE, id) values ($1, 'ZERO', $2)",
					"insert into BaseEntity (name, PARENT_ID, TYPE, id) values ($1, $2, 'TWO', $3)",
					"insert into BaseEntity (name, PARENT_ID, TYPE, id) values ($1, $2, 'ONE', $3)"
			};
		}
	}

	public static class UnorderedTest extends InsertOrderingReferenceSeveralDifferentSubclassBase {

		@Override
		public boolean isOrderInserts() {
			return false;
		}

		@Override
		public String[] getExpectedNativeQueries() {
			return new String[] {
					"insert into UnrelatedEntity (unrelatedValue, id) values ($1, $2)",
					"insert into BaseEntity (name, TYPE, id) values ($1, 'ZERO', $2)",
					"insert into BaseEntity (name, PARENT_ID, TYPE, id) values ($1, $2, 'TWO', $3)",
					"insert into BaseEntity (name, PARENT_ID, TYPE, id) values ($1, $2, 'ONE', $3)",
					"insert into UnrelatedEntity (unrelatedValue, id) values ($1, $2)",
					"insert into BaseEntity (name, PARENT_ID, TYPE, id) values ($1, $2, 'ONE', $3)",
					"insert into BaseEntity (name, PARENT_ID, TYPE, id) values ($1, $2, 'TWO', $3)"
			};
		}
	}

	/**
	 * @return the expected list of native queries
	 */
	public abstract String[] getExpectedNativeQueries();

	/**
	 * @return value of property {@link Settings#ORDER_INSERTS}
	 */
	public abstract boolean isOrderInserts();

	private SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty( Settings.ORDER_INSERTS, String.valueOf( isOrderInserts() ) );
		configuration.setProperty( Settings.STATEMENT_BATCH_SIZE, "10" );
		configuration.addAnnotatedClass( BaseEntity.class );
		configuration.addAnnotatedClass( SubclassZero.class );
		configuration.addAnnotatedClass( SubclassOne.class );
		configuration.addAnnotatedClass( SubclassTwo.class );
		configuration.addAnnotatedClass( UnrelatedEntity.class );

		// Construct a tracker that collects query statements via the SqlStatementLogger framework.
		// Pass in configuration properties to hand-off any actual logging properties
		sqlTracker = new SqlStatementTracker(
				InsertOrderingReferenceSeveralDifferentSubclassBase::onlyInserts,
				configuration.getProperties() );
		return configuration;
	}

	private static boolean onlyInserts(String s) {
		return s.toLowerCase().startsWith( "insert" );
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	@Test
	public void testSubclassReferenceChain(TestContext context) {
		UnrelatedEntity unrelatedEntity1 = new UnrelatedEntity();
		SubclassZero subclassZero = new SubclassZero( "SubclassZero" );
		SubclassOne subclassOne = new SubclassOne( "SubclassOne" );
		subclassOne.parent = subclassZero;
		SubclassTwo subclassTwo = new SubclassTwo( "SubclassTwo" );
		subclassTwo.parent = subclassOne;

		// add extra instances for the sake of volume
		UnrelatedEntity unrelatedEntity2 = new UnrelatedEntity();
		SubclassOne subclassOne2 = new SubclassOne( "SubclassOne2" );
		SubclassTwo subclassD2 = new SubclassTwo( "SubclassD2" );

		test( context, getMutinySessionFactory().withTransaction( session -> session
						.persistAll(
								unrelatedEntity1, subclassZero, subclassTwo, subclassOne, unrelatedEntity2, subclassOne2,
								subclassD2
						) )
				.chain( () -> getMutinySessionFactory()
						.withSession( s -> s.find( SubclassOne.class, subclassOne.id ) ) )
				.invoke( result -> {
					context.assertEquals( subclassOne.name, result.name );
					assertThat( sqlTracker.getLoggedQueries() ).containsExactly( getExpectedNativeQueries() );
				} )

		);
	}

	@Entity(name = "BaseEntity")
	@DiscriminatorColumn(name = "TYPE")
	@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
	public static abstract class BaseEntity {

		@Id
		@GeneratedValue
		public Long id;

		public String name;

		public BaseEntity() {
		}

		public BaseEntity(String name) {
			this.name = name;
		}
	}

	@Entity(name = "SubclassZero")
	@DiscriminatorValue("ZERO")
	public static class SubclassZero extends BaseEntity {

		public SubclassZero() {
		}

		public SubclassZero(String name) {
			super( name );
		}
	}

	@Entity(name = "SubclassOne")
	@DiscriminatorValue("ONE")
	public static class SubclassOne extends BaseEntity {

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "PARENT_ID")
		private SubclassZero parent;

		@OneToMany(fetch = FetchType.LAZY, cascade = CascadeType.PERSIST, orphanRemoval = true, mappedBy = "parent", targetEntity = SubclassTwo.class)
		private List<SubclassTwo> subclassTwoList = new ArrayList<>();

		public SubclassOne() {
		}

		public SubclassOne(String name) {
			super( name );
		}
	}

	@Entity(name = "SubclassTwo")
	@DiscriminatorValue("TWO")
	public static class SubclassTwo extends BaseEntity {

		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "PARENT_ID")
		private SubclassOne parent;

		public SubclassTwo() {
		}

		public SubclassTwo(String name) {
			super( name );
		}
	}

	@Entity(name = "UnrelatedEntity")
	public static class UnrelatedEntity {

		@Id
		@GeneratedValue
		private Long id;

		private String unrelatedValue;

		public UnrelatedEntity() {
		}
	}
}
