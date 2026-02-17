/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.io.Serializable;
import java.util.Objects;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.annotations.EnabledFor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Entity;
import jakarta.persistence.ForeignKey;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinColumns;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import org.assertj.core.api.Assertions;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.tool.schema.JdbcMetadataAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadataAccessStrategy.INDIVIDUALLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnabledFor(COCKROACHDB)
public abstract class SchemaUpdateCockroachDBTestBase extends BaseReactiveTest {

	@Timeout(value = 10, timeUnit = MINUTES)
	public static class IndividuallySchemaUpdateCockroachTestBaseTest extends SchemaUpdateCockroachDBTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	@Timeout(value = 10, timeUnit = MINUTES)
	public static class GroupedSchemaUpdateCockroachTestBaseTest extends SchemaUpdateCockroachDBTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, GROUPED.toString() );
			return configuration;
		}
	}

	protected Configuration constructConfiguration(String hbm2DdlOption) {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, hbm2DdlOption );
		configuration.setProperty( Settings.DEFAULT_SCHEMA, "public" );
		return configuration;
	}

	@BeforeEach
	@Override
	public void before(VertxTestContext context) {
		Configuration createHbm2ddlConf = constructConfiguration( "create" );
		createHbm2ddlConf.addAnnotatedClass( ASimpleFirst.class );
		createHbm2ddlConf.addAnnotatedClass( AOther.class );

		test( context, setupSessionFactory( createHbm2ddlConf )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@AfterEach
	@Override
	public void after(VertxTestContext context) {
		final Configuration dropHbm2ddlConf = constructConfiguration( "drop" );
		dropHbm2ddlConf.addAnnotatedClass( ASimpleNext.class );
		dropHbm2ddlConf.addAnnotatedClass( AOther.class );
		dropHbm2ddlConf.addAnnotatedClass( AAnother.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( dropHbm2ddlConf ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@Test
	public void testValidationSucceed(VertxTestContext context) {
		Configuration createHbm2ddlConf = constructConfiguration( "validate" );
		createHbm2ddlConf.addAnnotatedClass( ASimpleFirst.class );
		createHbm2ddlConf.addAnnotatedClass( AOther.class );

		test( context, setupSessionFactory( createHbm2ddlConf ) );
	}

	@Test
	public void testUpdate(VertxTestContext context) {
		// this test checks that the correct indexes and foreign constraints have been created
		// by looking at the content of the pg_catalog schema using the following native queries.
		final String indexDefinitionQuery =
				"select indexdef from pg_indexes where schemaname = 'public' and tablename = ? order by indexname";

		final String foreignKeyDefinitionQuery =
				"select pg_catalog.pg_get_constraintdef(f.oid, true) as condef " +
						"from pg_catalog.pg_constraint f, pg_catalog.pg_class c " +
						"where f.conname = ? and c.oid = f.conrelid and c.relname = ?";

		final ASimpleNext aSimple = new ASimpleNext();
		aSimple.aValue = 9;
		aSimple.aStringValue = "abc";
		aSimple.data = "Data";

		final AOther aOther = new AOther();
		aOther.id1 = 1;
		aOther.id2 = "other";
		aOther.anotherString = "another";

		final AAnother aAnother = new AAnother();
		aAnother.description = "description";

		aSimple.aOther = aOther;
		aSimple.aAnother = aAnother;

		final Configuration updateHbm2ddlConf = constructConfiguration( "update" );
		updateHbm2ddlConf.addAnnotatedClass( ASimpleNext.class );
		updateHbm2ddlConf.addAnnotatedClass( AOther.class );
		updateHbm2ddlConf.addAnnotatedClass( AAnother.class );
		test(
				context,
				setupSessionFactory( updateHbm2ddlConf )
						.thenCompose( v -> getSessionFactory()
								.withTransaction( (session, t) -> session.persist( aSimple ) ) )
						.thenCompose( v1 -> openSession()
								.thenCompose( s -> s
										.find( ASimpleNext.class, aSimple.id )
										.thenAccept( result -> {
											assertNotNull( result );
											assertEquals( aSimple.aValue, result.aValue );
											assertEquals( aSimple.aStringValue, result.aStringValue );
											assertEquals( aSimple.data, result.data );
											assertNotNull( result.aOther );
											assertEquals( aOther.id1, result.aOther.id1 );
											assertEquals( aOther.id2, result.aOther.id2 );
											assertEquals( aOther.anotherString, result.aOther.anotherString );
											assertNotNull( result.aAnother );
											assertEquals( aAnother.description, result.aAnother.description );
										} )
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery, String.class )
												.setParameter( 1, "asimple" )
												.getResultList()
												.thenAccept( list -> Assertions.assertThat( list ).containsExactlyInAnyOrder(
														"CREATE INDEX i_asimple_avalue_astringvalue ON postgres.public.asimple USING btree (avalue ASC, astringvalue DESC)",
														"CREATE INDEX i_asimple_avalue_data ON postgres.public.asimple USING btree (avalue DESC, data ASC)",
														"CREATE UNIQUE INDEX asimple_pkey ON postgres.public.asimple USING btree (id ASC)",
														"CREATE UNIQUE INDEX u_asimple_astringvalue ON postgres.public.asimple USING btree (astringvalue ASC)"
												) )
										)
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery, String.class )
												.setParameter( 1, "aother" )
												.getSingleResult()
												.thenAccept( result ->
																	 assertEquals(
																			 "CREATE UNIQUE INDEX aother_pkey ON postgres.public.aother USING btree (id1 ASC, id2 ASC)",
																			 result
																	 )
												)
										)
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery, String.class )
												.setParameter( 1, "aanother" )
												.getSingleResult()
												.thenAccept( result ->
																	 assertEquals(
																			 "CREATE UNIQUE INDEX aanother_pkey ON postgres.public.aanother USING btree (id ASC)",
																			 result
																	 )
												)
										)
										.thenCompose( v -> s.createNativeQuery(
												foreignKeyDefinitionQuery,
												String.class
													  )
															  .setParameter( 1, "fk_asimple_aother" )
															  .setParameter( 2, "asimple" )
															  .getSingleResult()
															  .thenAccept( result ->
																				   assertEquals(
																						   "FOREIGN KEY (id1, id2) REFERENCES aother(id1, id2)",
																						   result
																				   )
															  )
										)
										.thenCompose( v -> s.createNativeQuery(
												foreignKeyDefinitionQuery,
												String.class
													  )
															  .setParameter( 1, "fk_asimple_aanother" )
															  .setParameter( 2, "asimple" )
															  .getSingleResult()
															  .thenAccept( result ->
																				   assertEquals(
																						   "FOREIGN KEY (aanother_id) REFERENCES aanother(id)",
																						   result
																				   )
															  )
										)
								)
						)
		);
	}

	@Entity(name = "ASimple")
	@Table(name = "ASimple", indexes = @Index(
			name = "i_asimple_avalue_astringValue",
			columnList = "aValue ASC, aStringValue DESC"
	))
	public static class ASimpleFirst {
		@Id
		@GeneratedValue
		private Integer id;
		private Integer aValue;
		private String aStringValue;
		@ManyToOne(cascade = CascadeType.ALL)
		@JoinColumns(
				value = { @JoinColumn(name = "id1"), @JoinColumn(name = "id2") },
				foreignKey = @ForeignKey(name = "fk_asimple_aother")
		)
		private AOther aOther;
	}

	@Entity(name = "ASimple")
	@Table(name = "ASimple", indexes = {
			@Index(name = "i_asimple_avalue_astringValue", columnList = "aValue ASC, aStringValue DESC"),
			@Index(name = "i_asimple_avalue_data", columnList = "aValue DESC, data ASC")
	},
			uniqueConstraints = { @UniqueConstraint(name = "u_asimple_astringvalue", columnNames = "aStringValue") }
	)
	public static class ASimpleNext {
		@Id
		@GeneratedValue
		private Integer id;

		private Integer aValue;

		private String aStringValue;

		private String data;

		@ManyToOne(cascade = CascadeType.ALL)
		@JoinColumns(
				value = { @JoinColumn(name = "id1"), @JoinColumn(name = "id2") },
				foreignKey = @ForeignKey(name = "fk_asimple_aother")
		)
		private AOther aOther;

		@ManyToOne(cascade = CascadeType.ALL)
		@JoinColumn(foreignKey = @ForeignKey(name = "fk_asimple_aanother"))
		private AAnother aAnother;
	}

	@Entity(name = "AOther")
	@IdClass(AOtherId.class)
	public static class AOther {
		@Id
		private int id1;

		@Id
		private String id2;

		private String anotherString;
	}

	public static class AOtherId implements Serializable {
		private int id1;
		private String id2;

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			AOtherId aOtherId = (AOtherId) o;
			return id1 == aOtherId.id1 && id2.equals( aOtherId.id2 );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id1, id2 );
		}
	}

	@Entity(name = "AAnother")
	public static class AAnother {
		@Id
		@GeneratedValue
		private Integer id;

		private String description;
	}
}
