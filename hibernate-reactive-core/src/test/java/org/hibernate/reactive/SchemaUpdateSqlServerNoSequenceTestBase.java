/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.Objects;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.ForeignKey;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.tool.schema.spi.SchemaManagementException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadaAccessStrategy.INDIVIDUALLY;

public abstract class SchemaUpdateSqlServerNoSequenceTestBase extends BaseReactiveTest {

	public static class IndividuallySchemaUpdateSqlServerNoSequenceTestBase
			extends SchemaUpdateSqlServerNoSequenceTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	public static class GroupedSchemaUpdateSqlServerNoSequenceTestBase extends SchemaUpdateSqlServerNoSequenceTestBase {

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
		// TODO: Setting DEFAULT_CATALOG causes CREATE SEQUENCE statements to fail
		//       in Hibernate Reactive. They don't fail with ORM though.
		//       Error message is:
		//       io.vertx.mssqlclient.MSSQLException: {
		//       ... message=''CREATE SEQUENCE' does not allow specifying the database name as a prefix to the object name.' ...
		//       }
		//       Maybe JDBC strips off the the catalog (database) name?
		// DEFAULT_CATALOG needs to be set to something other than an empty string;
		// otherwise, NullPointerException is thrown.
		configuration.setProperty( Settings.DEFAULT_CATALOG, "master" );
		configuration.setProperty( Settings.DEFAULT_SCHEMA, "dbo" );
		return configuration;
	}

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.runOnlyFor( SQLSERVER );

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

	//TODO: I'm just checking that the validation fails because the table is missing, but we need more tests to check that
	//      it fails for other scenarios: missing column, wrong type (?) and so on. (I don't know exactly what cases `validate`
	//      actually checks).
	@Test
	public void testValidationFails(TestContext context) {
		Configuration createHbm2ddlConf = constructConfiguration( "validate" );
		createHbm2ddlConf.addAnnotatedClass( AAnother.class );

		test( context, setupSessionFactory( createHbm2ddlConf )
				.handle( (unused, throwable) -> {
					context.assertNotNull( throwable );
					context.assertEquals( throwable.getClass(), SchemaManagementException.class );
					context.assertEquals( throwable.getMessage(), "Schema-validation: missing table [master.dbo.AAnother]" );
					return null;
				} ) );
	}

	@Test
	public void testUpdate(TestContext context) {
		final String indexDefinitionQuery =
				"SELECT s.name as index_name, c.name column_name FROM sys.stats s " +
					"INNER JOIN sys.stats_columns sc " +
						"ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id " +
					"INNER JOIN sys.columns c " +
						"ON sc.object_id = c.object_id AND sc.column_id = c.column_id " +
					"WHERE OBJECT_NAME(s.object_id) = ? and s.name = ?";

		final String foreignKeyDefinitionQuery =
				"select table_name, column_name " +
						"from information_schema.key_column_usage " +
						"where table_schema = 'dbo' and " +
						"constraint_name = ? and " +
						"table_name = ? " +
						"order by ordinal_position";

		final ASimpleNext aSimple = new ASimpleNext();
		aSimple.id = 2;
		aSimple.aValue = 9;
		aSimple.aStringValue = "abc";
		aSimple.data = "Data";

		final AOther aOther = new AOther();
		aOther.id1 = 1;
		aOther.id2 = "other";
		aOther.anotherString = "another";

		final AAnother aAnother = new AAnother();
		aAnother.id = 3;
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
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery )
												.setParameter( 1, "ASimple" )
												.setParameter( 2, "i_asimple_avalue_astringValue" )
												.getResultList()
												.thenAccept( list -> {
													context.assertEquals( 2, list.size() );
													context.assertEquals( "i_asimple_avalue_astringValue", ((Object[])list.get(0))[0] );
													context.assertEquals( "aValue", ((Object[])list.get(0))[1] );
													context.assertEquals( "i_asimple_avalue_astringValue", ((Object[])list.get(1))[0] );
													context.assertEquals( "aStringValue", ((Object[])list.get(1))[1] );
												} )
										)
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery )
												.setParameter( 1, "ASimple" )
												.setParameter( 2, "i_asimple_avalue_data" )
												.getResultList()
												.thenAccept( list -> {
													context.assertEquals( 2, list.size() );
													context.assertEquals( "i_asimple_avalue_data", ((Object[])list.get(0))[0] );
													context.assertEquals( "aValue", ((Object[])list.get(0))[1] );
													context.assertEquals( "i_asimple_avalue_data", ((Object[])list.get(1))[0] );
													context.assertEquals( "data", ((Object[])list.get(1))[1] );
												} )
										)
										.thenCompose( v -> s.createNativeQuery( foreignKeyDefinitionQuery )
												.setParameter( 1, "fk_asimple_aother" )
												.setParameter( 2, "ASimple" )
												.getResultList()
												.thenAccept( results -> {
													context.assertEquals( 2, results.size() );
													context.assertEquals( "ASimple", ( (Object[]) results.get( 0 ) )[0] );
													context.assertEquals( "id1", ( (Object[]) results.get( 0 ) )[1] );
													context.assertEquals( "ASimple", ( (Object[]) results.get( 1 ) )[0] );
													context.assertEquals( "id2", ( (Object[]) results.get( 1 ) )[1] );
												} )
										)
										.thenCompose( v -> s.createNativeQuery( foreignKeyDefinitionQuery )
												.setParameter( 1, "fk_asimple_aanother" )
												.setParameter( 2, "ASimple" )
												.getSingleResult()
												.thenAccept( result -> {
													context.assertEquals( "ASimple", ( (Object[]) result )[0] );
													context.assertEquals( "aAnother_id", ( (Object[]) result )[1] );
												} )
										)
								) )
		);

	}

	@Entity(name = "ASimple")
	@Table(name = "ASimple", indexes = @Index(
			name = "i_asimple_avalue_astringValue",
			columnList = "aValue ASC, aStringValue DESC"
	))
	public static class ASimpleFirst {
		@Id
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
		private Integer id;

		private String description;
	}
}
