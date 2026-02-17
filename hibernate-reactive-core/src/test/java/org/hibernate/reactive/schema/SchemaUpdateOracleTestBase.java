/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.schema;

import java.io.Serializable;
import java.util.Objects;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.annotations.EnabledFor;
import org.hibernate.reactive.provider.Settings;

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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.tool.schema.JdbcMetadataAccessStrategy.GROUPED;
import static org.hibernate.tool.schema.JdbcMetadataAccessStrategy.INDIVIDUALLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnabledFor(ORACLE)
public abstract class SchemaUpdateOracleTestBase extends BaseReactiveTest {

	@Timeout(value = 10, timeUnit = MINUTES)
	public static class IndividuallySchemaUpdateOracleTest extends SchemaUpdateOracleTestBase {

		@Override
		protected Configuration constructConfiguration(String hbm2DdlOption) {
			final Configuration configuration = super.constructConfiguration( hbm2DdlOption );
			configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, INDIVIDUALLY.toString() );
			return configuration;
		}
	}

	@Timeout(value = 10, timeUnit = MINUTES)
	public static class GroupedSchemaUpdateOracleTest extends SchemaUpdateOracleTestBase {

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
		configuration.setProperty( Settings.DEFAULT_SCHEMA, "hreact" );
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
		final String indexDefinitionQuery =
				"select index_type, uniqueness, constraint_index from user_indexes " +
						"where table_owner = 'HREACT' " +
						"and table_name = ? and index_name = ?";

		final String foreignKeyQuery =
				"select constraint_name from user_constraints where owner = 'HREACT' and table_name = ? and constraint_type = 'R'";

		final ASimpleNext aSimple = new ASimpleNext();
		aSimple.aValue = 9;
		aSimple.aStringValue = "abc";
		aSimple.data = "some sample data";

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
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery )
												.setParameter( 1, "ASIMPLE" )
												.setParameter( 2, "I_ASIMPLE_AVALUE_DATA" )
												.getSingleResult()
												.thenAccept( result -> {
													final Object[] resultArray = (Object[]) result;
													assertEquals( "FUNCTION-BASED NORMAL", resultArray[0] );
													assertEquals( "NONUNIQUE", resultArray[1] );
													assertEquals( "NO", resultArray[2] );
												} )
										)
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery )
												.setParameter( 1, "ASIMPLE" )
												.setParameter( 2, "U_ASIMPLE_ASTRINGVALUE" )
												.getSingleResult()
												.thenAccept( result -> {
													final Object[] resultArray = (Object[]) result;
													assertEquals( "NORMAL", resultArray[0] );
													assertEquals( "UNIQUE", resultArray[1] );
													assertEquals( "YES", resultArray[2] );
												} )
										)
										.thenCompose( v -> s.createNativeQuery( indexDefinitionQuery )
												.setParameter( 1, "ASIMPLE" )
												.setParameter( 2, "I_ASIMPLE_AVALUE_ASTRINGVALUE" )
												.getSingleResult()
												.thenAccept( result -> {
													final Object[] resultArray = (Object[]) result;
													assertEquals( "FUNCTION-BASED NORMAL", resultArray[0] );
													assertEquals( "NONUNIQUE", resultArray[1] );
													assertEquals( "NO", resultArray[2] );
												} )
										)
										// check foreign keys
										.thenCompose( v -> s.createNativeQuery( foreignKeyQuery )
												.setParameter( 1, "ASIMPLE" )
												.getResultList()
												.thenAccept( list -> {
																 assertEquals(
																		 "FK_ASIMPLE_AOTHER",
																		 list.get( 0 )
																 );
																 assertEquals(
																		 "FK_ASIMPLE_AANOTHER",
																		 list.get( 1 )
																 );
															 }
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
