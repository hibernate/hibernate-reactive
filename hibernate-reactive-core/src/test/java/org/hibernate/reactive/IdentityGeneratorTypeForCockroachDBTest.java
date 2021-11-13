/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.testing.DatabaseSelectionRule.runOnlyFor;

/**
 * Test supported types for ids generated by CockroachDB
 *
 * @see IdentityGeneratorTest
 * @see IdentityGeneratorTypeTest
 */
public class IdentityGeneratorTypeForCockroachDBTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule runOnly = runOnlyFor( COCKROACHDB );

	/**
	 * When {@link AvailableSettings#USE_GET_GENERATED_KEYS} is enabled, different
	 * queries will be used for each datastore to get the id
	 */
	public static class EnableUseGetGeneratedKeys extends IdentityGeneratorTypeForCockroachDBTest {

		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( AvailableSettings.USE_GET_GENERATED_KEYS, "true" );
			return configuration;
		}
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		// It's the default but I want to highlight what we are testing
		configuration.setProperty( AvailableSettings.USE_GET_GENERATED_KEYS, "false" );
		configuration.addAnnotatedClass( IntegerTypeEntity.class );
		configuration.addAnnotatedClass( LongTypeEntity.class );
		configuration.addAnnotatedClass( ShortTypeEntity.class );
		return configuration;
	}

	@Test
	public void longIdentityType(TestContext context) {
		LongTypeEntity entity = new LongTypeEntity();

		test( context, getMutinySessionFactory()
				.withTransaction( (s, tx) -> s.persist( entity ) )
				.invoke( () -> {
					context.assertNotNull( entity );
					context.assertTrue( entity.id > 0 );
				} )
		);
	}

	@Test
	public void integerIdentityType(TestContext context) {
		Exception thrown = Assert.assertThrows(Exception.class, () ->
				test( context, getMutinySessionFactory()
					.withTransaction( (s, tx) -> s.persist( new IntegerTypeEntity() ) )
		) );

		Assert.assertTrue( thrown.getMessage().contains( "too big" ) );
		Assert.assertTrue( thrown.getMessage().contains( "Integer" ) );
	}

	@Test
	public void shortIdentityType(TestContext context) {
		Exception thrown = Assert.assertThrows(Exception.class, () ->
				test( context, getMutinySessionFactory()
					.withTransaction( (s, tx) -> s.persist( new ShortTypeEntity() ) )
		) );

		Assert.assertTrue( thrown.getMessage().contains( "too big" ) );
		Assert.assertTrue( thrown.getMessage().contains( "Short" ) );
	}

	interface TypeIdentity<T extends Number> {
		T getId();
	}

	@Entity
	@Table(name = "IntegerTypeEntity")
	static class IntegerTypeEntity implements TypeIdentity<Integer> {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Integer id;

		@Override
		public Integer getId() {
			return id;
		}
	}

	@Entity
	@Table(name = "LongTypeEntity")
	static class LongTypeEntity implements TypeIdentity<Long> {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Long id;

		@Override
		public Long getId() {
			return id;
		}
	}

	@Entity
	@Table(name = "ShortTypeEntity")
	static class ShortTypeEntity implements TypeIdentity<Short> {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Short id;

		@Override
		public Short getId() {
			return id;
		}
	}
}
