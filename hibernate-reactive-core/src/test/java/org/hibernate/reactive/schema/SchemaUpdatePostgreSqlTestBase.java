/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.io.Serializable;
import java.util.Objects;
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

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

public abstract class SchemaUpdatePostgreSqlTestBase extends BaseReactiveTest {

	public static class IndividuallySchemaUpdatePostgreSqlTestBase extends SchemaUpdatePostgreSqlTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedSchemaUpdatePostgreSqlTestBase extends SchemaUpdatePostgreSqlTestBase {

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

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Before
	@Override
	public void before(TestContext context) {
		Configuration createHbm2ddlConf = constructConfiguration( "create" );
		createHbm2ddlConf.addAnnotatedClass( ASimpleFirst.class );
		createHbm2ddlConf.addAnnotatedClass( AOther.class );

		test( context, setupSessionFactory( createHbm2ddlConf )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@After
	@Override
	public void after(TestContext context) {
		final Configuration dropHbm2ddlConf = constructConfiguration( "drop" );
		dropHbm2ddlConf.addAnnotatedClass( ASimpleNext.class );
		dropHbm2ddlConf.addAnnotatedClass( AOther.class );
		dropHbm2ddlConf.addAnnotatedClass( AAnother.class );

		test( context, factoryManager.stop()
				.thenCompose( v -> setupSessionFactory( dropHbm2ddlConf ) )
				.thenCompose( v -> factoryManager.stop() ) );
	}

	@Test
	public void testValidationSucceed(TestContext context) {
		Configuration createHbm2ddlConf = constructConfiguration( "validate" );
		createHbm2ddlConf.addAnnotatedClass( ASimpleFirst.class );
		createHbm2ddlConf.addAnnotatedClass( AOther.class );

		test( context, setupSessionFactory( createHbm2ddlConf ) );
	}

	@Test
	public void testUpdate(TestContext context) {
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
											context.assertNotNull( result );
											context.assertEquals( aSimple.aValue, result.aValue );
											context.assertEquals( aSimple.aStringValue, result.aStringValue );
											context.assertEquals( aSimple.data, result.data );
											context.assertNotNull( result.aOther );
											context.assertEquals( aOther.id1, result.aOther.id1 );
											context.assertEquals( aOther.id2, result.aOther.id2 );
											context.assertEquals( aOther.anotherString, result.aOther.anotherString );
											context.assertNotNull( result.aAnother );
											context.assertEquals( aAnother.description, result.aAnother.description );
										} )
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery, String.class )
												.setParameter( 1, "asimple" )
												.getResultList()
												.thenAccept( list -> {
													context.assertEquals(
															"CREATE UNIQUE INDEX asimple_pkey ON public.asimple USING btree (id)",
															list.get( 0 )
													);
													context.assertEquals(
															"CREATE INDEX i_asimple_avalue_astringvalue ON public.asimple USING btree (avalue, astringvalue DESC)",
															list.get( 1 )
													);
													context.assertEquals(
															"CREATE INDEX i_asimple_avalue_data ON public.asimple USING btree (avalue DESC, data)",
															list.get( 2 )
													);
													context.assertEquals(
															"CREATE UNIQUE INDEX u_asimple_astringvalue ON public.asimple USING btree (astringvalue)",
															list.get( 3 )
													);
												} )
										)
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery, String.class )
												.setParameter( 1, "aother" )
												.getSingleResult()
												.thenAccept( result ->
																	 context.assertEquals(
																			 "CREATE UNIQUE INDEX aother_pkey ON public.aother USING btree (id1, id2)",
																			 result
																	 )
												)
										)
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery, String.class )
												.setParameter( 1, "aanother" )
												.getSingleResult()
												.thenAccept( result ->
																	 context.assertEquals(
																			 "CREATE UNIQUE INDEX aanother_pkey ON public.aanother USING btree (id)",
																			 result
																	 )
												)
										)
										// check foreign keys
										.thenCompose( v -> s.createNativeQuery(
												foreignKeyDefinitionQuery,
												String.class
													  )
															  .setParameter( 1, "fk_asimple_aother" )
															  .setParameter( 2, "asimple" )
															  .getSingleResult()
															  .thenAccept( result ->
																				   context.assertEquals(
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
																				   context.assertEquals(
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
			@Index(name = "i_asimple_avalue_astringvalue", columnList = "aValue ASC, aStringValue DESC"),
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
