/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.OracleParameters;
import org.hibernate.reactive.pool.impl.PostgresParameters;
import org.hibernate.reactive.pool.impl.SQLServerParameters;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout( value = 5, timeUnit = TimeUnit.MINUTES )
public class BatchQueryOnConnectionTest extends BaseReactiveTest {

	private static final int BATCH_SIZE = 20;

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( DataPoint.class );
	}

	@Test
	public void testBatchInsertSizeEqMultiple(VertxTestContext context) {
		test( context, doBatchInserts( context, 50, BATCH_SIZE )
				.thenAccept( paramsBatches -> {
					assertEquals( 3, paramsBatches.size() );
					assertEquals( 20, paramsBatches.get( 0 ).size() );
					assertEquals( 20, paramsBatches.get( 1 ).size() );
					assertEquals( 10, paramsBatches.get( 2 ).size() );
				} )
		);
	}

	@Test
	public void testBatchInsertUpdateSizeLtMultiple(VertxTestContext context) {
		test( context, doBatchInserts( context, 50, BATCH_SIZE - 1 )
				.thenAccept( paramsBatches -> {
					assertEquals( 3, paramsBatches.size() );
					assertEquals( 19, paramsBatches.get( 0 ).size() );
					assertEquals( 19, paramsBatches.get( 1 ).size() );
					assertEquals( 12, paramsBatches.get( 2 ).size() );
				} )
		);
	}

	@Test
	public void testBatchInsertUpdateSizeGtMultiple(VertxTestContext context) {
		test( context, doBatchInserts( context, 50, BATCH_SIZE + 1 )
				.thenAccept( paramsBatches -> {
					assertEquals( 5, paramsBatches.size() );
					assertEquals( 20, paramsBatches.get( 0 ).size() );
					assertEquals( 1, paramsBatches.get( 1 ).size() );
					assertEquals( 20, paramsBatches.get( 2 ).size() );
					assertEquals( 1, paramsBatches.get( 3 ).size() );
					assertEquals( 8, paramsBatches.get( 4 ).size() );
				} )
		);
	}

	public CompletionStage<List<List<Object[]>>> doBatchInserts(VertxTestContext context, int nEntities, int nEntitiesMultiple) {
		final String insertSql = process( "insert into DataPoint (description, x, y, id) values (?, ?, ?, ?)" );

		List<List<Object[]>> paramsBatches = new ArrayList<>();
		List<Object[]> paramsBatch = new ArrayList<>( BATCH_SIZE );

		for ( int i = 1; i <= nEntities; i++ ) {
			DataPoint dp = new DataPoint( i );
			dp.description = "#" + i;
			dp.setX( new BigDecimal( i * 0.1d ).setScale( 19, RoundingMode.DOWN ) );
			dp.setY( new BigDecimal( dp.getX().doubleValue() * Math.PI ).setScale( 19, RoundingMode.DOWN ) );
			//uncomment to expose bug in DB2 client:
//			dp.setY(new BigDecimal(Math.cos(dp.getX().doubleValue())).setScale(19, BigDecimal.ROUND_DOWN));

			paramsBatch.add( new Object[] { dp.description, dp.x, dp.y, i } );

			if ( ( paramsBatch.size() == BATCH_SIZE ) || ( i % nEntitiesMultiple == 0 ) )  {
				paramsBatches.add( paramsBatch );
				paramsBatch = new ArrayList<>( BATCH_SIZE );
			}
		}

		if ( paramsBatch.size() > 0 ) {
			paramsBatches.add( paramsBatch );
		}

		CompletionStage<ReactiveConnection> stage = connection();
		for ( List<Object[]> batch : paramsBatches ) {
			stage = stage.thenCompose( connection -> connection
					.update( insertSql, batch )
					.thenApply( updateCounts -> {
						assertEquals( batch.size(), updateCounts.length );
						for ( int updateCount : updateCounts ) {
							assertEquals( 1, updateCount );
						}
						return connection;
					} ) );
		}

		return stage
				.thenCompose( ignore -> openSession() )
				.thenCompose( s -> s
						.createQuery( "select count(*) from DataPoint" ).getSingleResult()
						.thenAccept( result -> assertEquals( (long) nEntities, result ) ) )
				.thenApply( v -> paramsBatches );
	}

	private String process(String sql) {
		switch ( dbType() ) {
			case POSTGRESQL:
			case COCKROACHDB:
				return PostgresParameters.INSTANCE.process( sql );
			case SQLSERVER:
				return SQLServerParameters.INSTANCE.process( sql );
			case ORACLE:
				return OracleParameters.INSTANCE.process( sql );
			default:
				return sql;
		}
	}

	@Entity(name = "DataPoint")
	public static class DataPoint {
		@Id
		private int id;
		private BigDecimal x;
		private BigDecimal y;
		private String description;

		public DataPoint() {
		}

		public DataPoint(int id) {
			this.id = id;
		}
		/**
		 * @return Returns the description.
		 */
		public String getDescription() {
			return description;
		}
		/**
		 * @param description The description to set.
		 */
		public void setDescription(String description) {
			this.description = description;
		}
		/**
		 * @return Returns the id.
		 */
		public int getId() {
			return id;
		}
		/**
		 * @param id The id to set.
		 */
		public void setId(int id) {
			this.id = id;
		}
		/**
		 * @return Returns the x.
		 */
		public BigDecimal getX() {
			return x;
		}
		/**
		 * @param x The x to set.
		 */
		public void setX(BigDecimal x) {
			this.x = x;
		}
		/**
		 * @return Returns the y.
		 */
		public BigDecimal getY() {
			return y;
		}
		/**
		 * @param y The y to set.
		 */
		public void setY(BigDecimal y) {
			this.y = y;
		}
	}
}
