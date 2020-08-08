/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.Session;
import org.hibernate.StaleObjectStateException;
import org.hibernate.bytecode.enhance.spi.interceptor.LazyAttributeDescriptor;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL81Dialect;
import org.hibernate.engine.OptimisticLockStyle;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.PersistentAttributeInterceptable;
import org.hibernate.engine.spi.PersistentAttributeInterceptor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.jdbc.Expectation;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.JoinedSubclassEntityPersister;
import org.hibernate.persister.entity.Lockable;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.loader.entity.impl.ReactiveDynamicBatchingEntityLoaderBuilder;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveSession;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.Delete;
import org.hibernate.sql.SimpleSelect;
import org.hibernate.sql.Update;
import org.hibernate.tuple.InMemoryValueGenerationStrategy;
import org.hibernate.type.Type;
import org.hibernate.type.VersionType;
import org.jboss.logging.Logger;

import javax.persistence.metamodel.Attribute;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static org.hibernate.jdbc.Expectations.appropriateExpectation;
import static org.hibernate.pretty.MessageHelper.infoString;

/**
 * An abstract implementation of {@link ReactiveEntityPersister} whose
 * concrete implementations each extend a concrete subclass of
 * {@link AbstractEntityPersister}. Note that there are three
 * main flavors of {@code AbstractEntityPersister}, one for each
 * of the three inheritance mapping strategies, and thus we have
 * three flavors of {@link ReactiveEntityPersister}. Therefore, this
 * interface is defined as a mixin. This design avoid duplicating
 * the code in this class in the three different subclasses.
 *
 * Concrete implementations of this interface _must_ also extend
 * {@code AbstractEntityPersister} or one of its concrete
 * subclasses.
 *
 * @see ReactiveJoinedSubclassEntityPersister
 * @see ReactiveUnionSubclassEntityPersister
 * @see ReactiveSingleTableEntityPersister
 */
public interface ReactiveAbstractEntityPersister extends ReactiveEntityPersister, OuterJoinLoadable, Lockable {
	Logger log = Logger.getLogger( JoinedSubclassEntityPersister.class );

	/**
	 * A self-reference of type {@code AbstractEntityPersister}.
	 *
	 * @return this object
	 */
	default AbstractEntityPersister delegate() {
		return (AbstractEntityPersister) this;
	}

	default ReactiveConnection getReactiveConnection(SharedSessionContractImplementor session) {
		return ((ReactiveSession) session).getReactiveConnection();
	}

	@Override
	default CompletionStage<Serializable> insertReactive(Object[] fields, Object object,
														 SharedSessionContractImplementor session) {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session );

		final int span = delegate().getTableSpan();
		CompletionStage<Serializable> stage = CompletionStages.nullFuture();
		if ( delegate().getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = delegate().getPropertiesToInsert( fields );
			stage = stage.thenCompose( n -> insertReactive( fields, notNull, delegate().generateInsertString( true, notNull ), session ) );
			for ( int j = 1; j < span; j++ ) {
				final int jj = j;
				stage = stage.thenCompose( id ->
						insertReactive(id, fields, notNull, jj, delegate().generateInsertString(notNull, jj), session)
							.thenApply( v -> id ));
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			stage = stage.thenCompose( n -> insertReactive( fields, delegate().getPropertyInsertability(), delegate().getSQLIdentityInsertString(), session ) );
			for ( int j = 1; j < span; j++ ) {
				final int jj = j;
				stage = stage.thenCompose( id ->
						insertReactive(id, fields, delegate().getPropertyInsertability(), jj, delegate().getSQLInsertStrings()[jj], session)
								.thenApply( v -> id ));
			}
		}
		return stage;
	}

	void preInsertInMemoryValueGeneration(Object[] fields, Object object,
										  SharedSessionContractImplementor session);

	@Override
	default CompletionStage<?> insertReactive(
			Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session) {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session );

		CompletionStage<?> insertStage = CompletionStages.nullFuture();
		final int span = delegate().getTableSpan();
		if ( delegate().getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = delegate().getPropertiesToInsert( fields );
			for ( int j = 0; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose(
						v -> insertReactive(
								id,
								fields,
								notNull,
								jj,
								delegate().generateInsertString( notNull, jj ),
								session
						));
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			for ( int j = 0; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose(
						v -> insertReactive(
								id,
								fields,
								delegate().getPropertyInsertability(),
								jj,
								delegate().getSQLInsertStrings()[jj],
								session
						));
			}
		}
		return insertStage;
	}

	default CompletionStage<?> insertReactive(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			SharedSessionContractImplementor session) {

		if ( delegate().isInverseTable( j ) ) {
			return CompletionStages.nullFuture();
		}

		//note: it is conceptually possible that a UserType could map null to
		//	  a non-null value, so the following is arguable:
		if ( delegate().isNullableTable( j ) && delegate().isAllNull( fields, j ) ) {
			return CompletionStages.nullFuture();
		}

		if ( log.isTraceEnabled() ) {
			log.tracev( "Inserting entity: {0}", infoString(delegate(), id, delegate().getFactory() ) );
			if ( j == 0 && delegate().isVersioned() ) {
				log.tracev( "Version: {0}", Versioning.getVersion( fields, delegate()) );
			}
		}

		// TODO : shouldn't inserts be Expectations.NONE?
		final Expectation expectation = appropriateExpectation( delegate().getInsertResultCheckStyles()[j] );
		boolean useBatch = expectation.canBeBatched() && getIdentifierGenerator().supportsJdbcBatchInserts();
//		final boolean callable = delegate.isInsertCallable( j );

		Object[] params = PreparedStatementAdaptor.bind( insert -> {
			boolean[][] insertable = delegate().getPropertyColumnInsertable();
			int index = delegate().dehydrate( null, fields, notNull, insertable, j, insert, session, false );
			delegate().getIdentifierType().nullSafeSet( insert, id, index, session );
		} );

		return getReactiveConnection( session )
				.update( sql, params, useBatch, new InsertExpectation( expectation, this ) );
	}

	/**
	 * Perform an SQL INSERT, and then retrieve a generated identifier.
	 * <p>
	 * This form is used for PostInsertIdentifierGenerator-style ids.
	 */
	default CompletionStage<Serializable> insertReactive(
			Object[] fields,
			boolean[] notNull,
			String sql,
			SharedSessionContractImplementor session) {

		if ( log.isTraceEnabled() ) {
			log.tracev( "Inserting entity: {0}", infoString(delegate()) );
			if ( delegate().isVersioned() ) {
				log.tracev( "Version: {0}", Versioning.getVersion( fields, delegate()) );
			}
		}

		Object[] params = PreparedStatementAdaptor.bind( insert -> {
			boolean[][] insertable = delegate().getPropertyColumnInsertable();
			delegate().dehydrate( null, fields, notNull, insertable, 0, insert, session, false );
		} );

		SessionFactoryImplementor factory = session.getFactory();
		Dialect dialect = factory.getJdbcServices().getDialect();
		String identifierColumnName = delegate().getIdentifierColumnNames()[0];
		ReactiveConnection connection = getReactiveConnection(session);
		if ( factory.getSessionFactoryOptions().isGetGeneratedKeysEnabled() ) {
			//TODO: wooooo this is awful ... I believe the problem is fixed in Hibernate 6
			if ( dialect instanceof PostgreSQL81Dialect) {
				sql = sql + " returning " + identifierColumnName;
			}
			return connection.updateReturning( sql, params ).thenApply( id -> id );
		}
		else {
			//use an extra round trip to fetch the id
			String selectIdSql = dialect.getIdentityColumnSupport()
					.getIdentitySelectString(
							delegate().getTableName(),
							identifierColumnName,
							Types.INTEGER
					);
			return connection.update( sql, params )
					.thenCompose( v -> connection.selectLong( selectIdSql, new Object[0] ) )
					.thenApply( id -> id );
		}

	}

	default CompletionStage<?> deleteReactive(
			Serializable id,
			Object version,
			int j,
			String sql,
			SharedSessionContractImplementor session,
			Object[] loadedState) {

		if ( delegate().isInverseTable( j ) ) {
			return CompletionStages.nullFuture();
		}
		final boolean useVersion = j == 0 && delegate().isVersioned();
//		final boolean callable = delegate.isDeleteCallable( j );
		final Expectation expectation = appropriateExpectation( delegate().getDeleteResultCheckStyles()[j] );
		final boolean useBatch = j == 0 && isBatchable() && expectation.canBeBatched();

		if ( log.isTraceEnabled() ) {
			log.tracev( "Deleting entity: {0}", infoString(delegate(), id, delegate().getFactory() ) );
			if ( useVersion ) {
				log.tracev( "Version: {0}", version );
			}
		}

		if ( delegate().isTableCascadeDeleteEnabled( j ) ) {
			if ( log.isTraceEnabled() ) {
				log.tracev( "Delete handled by foreign key constraint: {0}", delegate().getTableName( j ) );
			}
			//EARLY EXIT!
			return CompletionStages.nullFuture();
		}

		//Render the SQL query
		Object[] params = PreparedStatementAdaptor.bind( delete -> {
			int index = 1;

			index += expectation.prepare( delete );

			// Do the key. The key is immutable so we can use the _current_ object state - not necessarily
			// the state at the time the delete was issued
			delegate().getIdentifierType().nullSafeSet( delete, id, index, session );
			index += delegate().getIdentifierColumnSpan();

			// We should use the _current_ object state (ie. after any updates that occurred during flush)
			if ( useVersion ) {
				delegate().getVersionType().nullSafeSet( delete, version, index, session );
			}
			else if ( isAllOrDirtyOptimisticLocking() && loadedState != null ) {
				boolean[] versionability = delegate().getPropertyVersionability();
				Type[] types = delegate().getPropertyTypes();
				for (int i = 0; i < delegate().getEntityMetamodel().getPropertySpan(); i++ ) {
					if ( delegate().isPropertyOfTable( i, j ) && versionability[i] ) {
						// this property belongs to the table and it is not specifically
						// excluded from optimistic locking by optimistic-lock="false"
						boolean[] settable = types[i].toColumnNullness( loadedState[i], delegate().getFactory() );
						types[i].nullSafeSet( delete, loadedState[i], index, settable, session );
						index += ArrayHelper.countTrue( settable );
					}
				}
			}
		} );

		return getReactiveConnection( session )
				.update( sql, params, useBatch, new DeleteExpectation( id, expectation, this ) );
	}

	default CompletionStage<?> deleteReactive(
			Serializable id, Object version, Object object,
			SharedSessionContractImplementor session) {
		final int span = delegate().getTableSpan();
		boolean isImpliedOptimisticLocking = !delegate().getEntityMetamodel().isVersioned() && isAllOrDirtyOptimisticLocking();
		Object[] loadedState = null;
		if ( isImpliedOptimisticLocking ) {
			// need to treat this as if it where optimistic-lock="all" (dirty does *not* make sense);
			// first we need to locate the "loaded" state
			//
			// Note, it potentially could be a proxy, so doAfterTransactionCompletion the location the safe way...
			final EntityKey key = session.generateEntityKey( id, delegate());
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			Object entity = persistenceContext.getEntity( key );
			if ( entity != null ) {
				EntityEntry entry = persistenceContext.getEntry( entity );
				loadedState = entry.getLoadedState();
			}
		}

		final String[] deleteStrings;
		if ( isImpliedOptimisticLocking && loadedState != null ) {
			// we need to utilize dynamic delete statements
			deleteStrings = generateSQLDeleteStrings( loadedState );
		}
		else {
			// otherwise, utilize the static delete statements
			deleteStrings = delegate().getSQLDeleteStrings();
		}

		CompletionStage<?> deleteStage = CompletionStages.nullFuture();
		for ( int j = span - 1; j >= 0; j-- ) {
			// For now we assume there is only one delete query
			int jj = j;
			Object[] state = loadedState;
			deleteStage = deleteStage.thenCompose(
					v-> deleteReactive(
							id,
							version,
							jj,
							deleteStrings[jj],
							session,
							state
					));
		}

		return deleteStage;
	}

	default boolean isAllOrDirtyOptimisticLocking() {
		OptimisticLockStyle optimisticLockStyle =
				delegate().getEntityMetamodel().getOptimisticLockStyle();
		return optimisticLockStyle == OptimisticLockStyle.DIRTY
				|| optimisticLockStyle == OptimisticLockStyle.ALL;
	}

	default String[] generateSQLDeleteStrings(Object[] loadedState) {
		int span = delegate().getTableSpan();
		String[] deleteStrings = new String[span];
		for ( int j = span - 1; j >= 0; j-- ) {
			Delete delete = new Delete()
					.setTableName( delegate().getTableName( j ) )
					.addPrimaryKeyColumns( delegate().getKeyColumns( j ) );
			if ( delegate().getFactory().getSessionFactoryOptions().isCommentsEnabled() ) {
				delete.setComment( "delete " + delegate().getEntityName() + " [" + j + "]" );
			}

			boolean[] versionability = delegate().getPropertyVersionability();
			Type[] types = delegate().getPropertyTypes();
			for (int i = 0; i < delegate().getEntityMetamodel().getPropertySpan(); i++ ) {
				if ( delegate().isPropertyOfTable( i, j ) && versionability[i] ) {
					// this property belongs to the table and it is not specifically
					// excluded from optimistic locking by optimistic-lock="false"
					String[] propertyColumnNames = delegate().getPropertyColumnNames( i );
					boolean[] propertyNullness = types[i].toColumnNullness( loadedState[i], delegate().getFactory() );
					for ( int k = 0; k < propertyNullness.length; k++ ) {
						if ( propertyNullness[k] ) {
							delete.addWhereFragment( propertyColumnNames[k] + " = $" + ( k + 1 ) );
						}
						else {
							delete.addWhereFragment( propertyColumnNames[k] + " is null" );
						}
					}
				}
			}
			deleteStrings[j] = delete.toStatementString();
		}
		return deleteStrings;
	}

	default CompletionStage<Boolean> updateReactive(
			final Serializable id,
			final Object[] fields,
			final Object[] oldFields,
			final Object rowId,
			final boolean[] includeProperty,
			final int j,
			final Object oldVersion,
			final String sql,
			final SharedSessionContractImplementor session) {

		final Expectation expectation = appropriateExpectation( delegate().getUpdateResultCheckStyles()[j] );
		final boolean useBatch = expectation.canBeBatched() && isBatchable();
//		final boolean callable = delegate.isUpdateCallable( j );
		final boolean useVersion = j == 0 && delegate().isVersioned();

		if ( log.isTraceEnabled() ) {
			log.tracev( "Updating entity: {0}", infoString(delegate(), id, delegate().getFactory() ) );
			if ( useVersion ) {
				log.tracev( "Existing version: {0} -> New version:{1}", oldVersion, fields[delegate().getVersionProperty()] );
			}
		}

		Object[] params = PreparedStatementAdaptor.bind( update -> {
			int index = 1;
			index += expectation.prepare( update );

			//Now write the values of fields onto the prepared statement
			index = delegate().dehydrate(
					id,
					fields,
					rowId,
					includeProperty,
					delegate().getPropertyColumnUpdateable(),
					j,
					update,
					session,
					index,
					true
			);

			// Write any appropriate versioning conditional parameters
			if ( useVersion && delegate().getEntityMetamodel().getOptimisticLockStyle() == OptimisticLockStyle.VERSION ) {
				if ( delegate().checkVersion( includeProperty ) ) {
					delegate().getVersionType().nullSafeSet( update, oldVersion, index, session );
				}
			}
			else if ( isAllOrDirtyOptimisticLocking() && oldFields != null ) {
				boolean[] versionability = delegate().getPropertyVersionability(); //TODO: is this really necessary????
				boolean[] includeOldField = delegate().getEntityMetamodel().getOptimisticLockStyle() == OptimisticLockStyle.ALL
						? delegate().getPropertyUpdateability()
						: includeProperty;
				Type[] types = delegate().getPropertyTypes();
				for (int i = 0; i < delegate().getEntityMetamodel().getPropertySpan(); i++ ) {
					boolean include = includeOldField[i] &&
							delegate().isPropertyOfTable( i, j ) &&
							versionability[i]; //TODO: is this really necessary????
					if ( include ) {
						boolean[] settable = types[i].toColumnNullness( oldFields[i], delegate().getFactory() );
						types[i].nullSafeSet(
								update,
								oldFields[i],
								index,
								settable,
								session
						);
						index += ArrayHelper.countTrue( settable );
					}
				}
			}
		} );

		UpdateExpectation result = new UpdateExpectation(id, expectation, this);
		return getReactiveConnection( session )
				.update( sql, params, useBatch, result )
				.thenApply( v -> useBatch || result.isSuccessful() );
	}

	boolean check(
			int rows,
			Serializable id,
			int tableNumber,
			Expectation expectation,
			PreparedStatement statement, String sql) throws HibernateException;

	default CompletionStage<?> updateReactive(
			final Serializable id,
			final Object[] fields,
			int[] dirtyFields,
			final boolean hasDirtyCollection,
			final Object[] oldFields,
			final Object oldVersion,
			final Object object,
			final Object rowId,
			final SharedSessionContractImplementor session) {

		// apply any pre-update in-memory value generation
		if ( delegate().getEntityMetamodel().hasPreUpdateGeneratedValues() ) {
			final InMemoryValueGenerationStrategy[] valueGenerationStrategies =
					delegate().getEntityMetamodel().getInMemoryValueGenerationStrategies();
			int valueGenerationStrategiesSize = valueGenerationStrategies.length;
			if ( valueGenerationStrategiesSize != 0 ) {
				int[] fieldsPreUpdateNeeded = new int[valueGenerationStrategiesSize];
				int count = 0;
				for ( int i = 0; i < valueGenerationStrategiesSize; i++ ) {
					if ( valueGenerationStrategies[i] != null
							&& valueGenerationStrategies[i].getGenerationTiming().includesUpdate() ) {
						fields[i] = valueGenerationStrategies[i].getValueGenerator().generateValue(
								(Session) session,
								object
						);
						delegate().setPropertyValue( object, i, fields[i] );
						fieldsPreUpdateNeeded[count++] = i;
					}
				}
				if ( dirtyFields != null ) {
					dirtyFields = ArrayHelper.join( dirtyFields, ArrayHelper.trim( fieldsPreUpdateNeeded, count ) );
				}
			}
		}

		//note: dirtyFields==null means we had no snapshot, and we couldn't get one using select-before-update
		//	  oldFields==null just means we had no snapshot to begin with (we might have used select-before-update to get the dirtyFields)

		final boolean[] tableUpdateNeeded = delegate().getTableUpdateNeeded( dirtyFields, hasDirtyCollection );
		final int span = delegate().getTableSpan();

		final boolean[] propsToUpdate;
		final String[] updateStrings;
		EntityEntry entry = session.getPersistenceContextInternal().getEntry( object );

		// Ensure that an immutable or non-modifiable entity is not being updated unless it is
		// in the process of being deleted.
		if ( entry == null && !delegate().isMutable() ) {
			throw new IllegalStateException( "Updating immutable entity that is not in session yet!" );
		}
		if ( ( delegate().getEntityMetamodel().isDynamicUpdate() && dirtyFields != null ) ) {
			// We need to generate the UPDATE SQL when dynamic-update="true"
			propsToUpdate = delegate().getPropertiesToUpdate( dirtyFields, hasDirtyCollection );
			// don't need to check laziness (dirty checking algorithm handles that)
			updateStrings = new String[span];
			for ( int j = 0; j < span; j++ ) {
				updateStrings[j] = tableUpdateNeeded[j] ?
						delegate().generateUpdateString( propsToUpdate, j, oldFields, j == 0 && rowId != null ) :
						null;
			}
		}
		else if ( !delegate().isModifiableEntity( entry ) ) {
			// We need to generate UPDATE SQL when a non-modifiable entity (e.g., read-only or immutable)
			// needs:
			// - to have references to transient entities set to null before being deleted
			// - to have version incremented do to a "dirty" association
			// If dirtyFields == null, then that means that there are no dirty properties to
			// to be updated; an empty array for the dirty fields needs to be passed to
			// getPropertiesToUpdate() instead of null.
			propsToUpdate = delegate().getPropertiesToUpdate(
					( dirtyFields == null ? ArrayHelper.EMPTY_INT_ARRAY : dirtyFields ),
					hasDirtyCollection
			);
			// don't need to check laziness (dirty checking algorithm handles that)
			updateStrings = new String[span];
			for ( int j = 0; j < span; j++ ) {
				updateStrings[j] = tableUpdateNeeded[j] ?
						delegate().generateUpdateString( propsToUpdate, j, oldFields, j == 0 && rowId != null ) :
						null;
			}
		}
		else {
			// For the case of dynamic-update="false", or no snapshot, we use the static SQL
			boolean hasUninitializedLazy = delegate().hasUninitializedLazyProperties( object );
			updateStrings = getUpdateStrings( rowId != null, hasUninitializedLazy );
			propsToUpdate = delegate().getPropertyUpdateability( object );
		}

		CompletionStage<?> updateStage = CompletionStages.nullFuture();
		for ( int j = 0; j < span; j++ ) {
			// Now update only the tables with dirty properties (and the table with the version number)
			if ( tableUpdateNeeded[j] ) {
				// We assume there is only one table for now
				final int jj = j;
				updateStage = updateStage.thenCompose(
						v -> updateOrInsertReactive(
								id,
								fields,
								oldFields,
								jj == 0 ? rowId : null,
								propsToUpdate,
								jj,
								oldVersion,
								updateStrings[jj],
								session
						)
				);
			}
		}
		return updateStage;
	}

	String[] getUpdateStrings(boolean byRowId, boolean hasUninitializedLazyProperties);

	default CompletionStage<?> updateOrInsertReactive(
			final Serializable id,
			final Object[] fields,
			final Object[] oldFields,
			final Object rowId,
			final boolean[] includeProperty,
			final int j,
			final Object oldVersion,
			final String sql,
			final SharedSessionContractImplementor session) {

		if ( !delegate().isInverseTable( j ) ) {

			if ( delegate().isNullableTable( j ) && delegate().isAllNull( oldFields, j ) && oldFields != null ) {
				// don't bother trying to update, we know there is no row there yet
				if ( !delegate().isAllNull( fields, j ) ) {
					return insertReactive( id, fields, delegate().getPropertyInsertability(), j, delegate().getSQLInsertStrings()[j], session );
				}
			}
			else if ( delegate().isNullableTable( j ) && delegate().isAllNull( fields, j ) ) {
				// All fields are null, we can just delete the row
				return deleteReactive( id, oldVersion, j, delegate().getSQLDeleteStrings()[j], session, null );
			}
			else {
				return updateReactive( id, fields, oldFields, rowId, includeProperty, j, oldVersion, sql, session )
						.thenApply( updated -> {
							if ( !updated && !delegate().isAllNull( fields, j ) ) {
								// Nothing has been updated because the row isn't in the db
								// Run an insert instead
								return insertReactive( id, fields, delegate().getPropertyInsertability(), j, delegate().getSQLInsertStrings()[j], session );
							}
							return null;
						} );
			}
		}

		// Nothing to do;
		return CompletionStages.nullFuture();
	}

	default String generateSelectLockString(LockOptions lockOptions) {
		final SessionFactoryImplementor factory = getFactory();
		Dialect dialect = factory.getJdbcServices().getDialect();
		final SimpleSelect select = new SimpleSelect(dialect)
				.setLockOptions( lockOptions )
				.setTableName( getRootTableName() )
				.addColumn( getRootTableIdentifierColumnNames()[0] )
				.addCondition( getRootTableIdentifierColumnNames(), "=?" );
		if ( isVersioned() ) {
			select.addCondition( getVersionColumnName(), "=?" );
		}
		if ( factory.getSessionFactoryOptions().isCommentsEnabled() ) {
			select.setComment( lockOptions.getLockMode() + " lock " + getEntityName() );
		}
		return select.toStatementString();
	}

	default String generateUpdateLockString(LockOptions lockOptions) {
		final SessionFactoryImplementor factory = getFactory();
		Dialect dialect = factory.getJdbcServices().getDialect();
		final Update update = new Update(dialect);
		update.setTableName( getRootTableName() );
		update.addPrimaryKeyColumns( getRootTableIdentifierColumnNames() );
		update.setVersionColumnName( getVersionColumnName() );
		update.addColumn( getVersionColumnName() );
		if ( factory.getSessionFactoryOptions().isCommentsEnabled() ) {
			update.setComment( lockOptions.getLockMode() + " lock " + getEntityName() );
		}
		return update.toStatementString();
	}

	@Override
	default CompletionStage<?> lockReactive(
			Serializable id,
			Object version,
			Object object,
			LockOptions lockOptions,
			SharedSessionContractImplementor session) throws HibernateException {

		LockMode lockMode = lockOptions.getLockMode();

		Object nextVersion = nextVersionForLock( lockMode, id, version, object, session );

		String sql;
		boolean writeLock;
		switch (lockMode) {
			case READ:
			case PESSIMISTIC_READ:
			case PESSIMISTIC_WRITE:
			case UPGRADE_NOWAIT:
			case UPGRADE_SKIPLOCKED:
			case UPGRADE:
				sql = generateSelectLockString( lockOptions );
				writeLock = false;
				break;
			case PESSIMISTIC_FORCE_INCREMENT:
			case FORCE:
			case WRITE:
				sql = generateUpdateLockString( lockOptions );
				writeLock = true;
				break;
			case NONE:
				return CompletionStages.nullFuture();
			default:
				throw new IllegalArgumentException("lock mode not supported");
		}

		PreparedStatementAdaptor statement = new PreparedStatementAdaptor();
		try {
			int offset = 1;
			if ( writeLock ) {
				getVersionType().nullSafeSet( statement, nextVersion, offset, session );
				offset++;
			}
			getIdentifierType().nullSafeSet( statement, id, offset, session );
			offset += getIdentifierType().getColumnSpan( getFactory() );
			if ( isVersioned() ) {
				getVersionType().nullSafeSet( statement, version, offset, session );
			}
		}
		catch ( SQLException e) {
			throw new HibernateException( e );
		}
		Object[] parameters = statement.getParametersAsArray();

		ReactiveConnection connection = getReactiveConnection( session );
		CompletionStage<Boolean> lock;
		if (writeLock) {
			lock = connection.update(sql, parameters).thenApply(affected -> affected > 0);
		}
		else {
			lock = connection.select(sql, parameters).thenApply(Iterator::hasNext);
		}

		return lock.thenAccept( found -> {
			if (!found) {
				throw new StaleObjectStateException( getEntityName(), id );
			}
		} ).handle( (r ,e) -> {
			CompletionStages.logSqlException( e,
					() -> "could not lock: "
							+ infoString( this, id, getFactory() ),
					sql
			);
			return CompletionStages.returnOrRethrow( e, r );
		} );
	}

	@Override
	VersionType<Object> getVersionType();

	default Object nextVersionForLock(LockMode lockMode, Serializable id, Object version, Object entity,
									  SharedSessionContractImplementor session) {
		if ( lockMode == LockMode.PESSIMISTIC_FORCE_INCREMENT ) {
			if ( !isVersioned() ) {
				throw new IllegalArgumentException("increment locks not supported for unversioned entity");
			}

			VersionType<Object> versionType = getVersionType();
			Object nextVersion = versionType.next( version, session);

			if ( log.isTraceEnabled() ) {
				log.trace(
						"Forcing version increment [" + infoString( this, id, getFactory() ) + "; "
								+ versionType.toLoggableString( version, getFactory() ) + " -> "
								+ versionType.toLoggableString( nextVersion, getFactory() ) + "]"
				);
			}

			session.getPersistenceContextInternal().getEntry( entity ).forceLocked( entity, nextVersion );

			return nextVersion;
		}
		else {
			return version;
		}
	}

	default CompletionStage<Object> reactiveLoad(Serializable id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session) {
		return reactiveLoad( id, optionalObject, lockOptions, session, null );
	}

	default CompletionStage<Object> reactiveLoad(Serializable id, Object optionalObject, LockOptions lockOptions, SharedSessionContractImplementor session, Boolean readOnly) {
		if ( log.isTraceEnabled() ) {
			log.tracev( "Fetching entity: {0}", infoString( this, id, getFactory() ) );
		}
		return getAppropriateLoader( lockOptions, session ).load( id, optionalObject, session, lockOptions, readOnly );
	}

	@Override
	default CompletionStage<List<Object>> reactiveMultiLoad(Serializable[] ids, SessionImplementor session, MultiLoadOptions loadOptions) {
		return ReactiveDynamicBatchingEntityLoaderBuilder.INSTANCE.multiLoad(this, ids, session, loadOptions);
	}

//	@Override
//	default CompletionStage<Boolean> reactiveIsTransient(Object entity, SessionImplementor session) {
//		Boolean unsaved = delegate().isTransient( entity, session );
//		if ( unsaved!=null ) {
//			return CompletionStages.completedFuture( unsaved );
//		}
//		String sql = processParameters(
//				delegate().getSQLSnapshotSelectString(),
//				session.getFactory().getJdbcServices().getDialect()
//		);
//		Serializable id = delegate().getIdentifier( entity, session );
//		Object[] params = toParameterArray( new QueryParameters( getIdentifierType(), id ), session );
//		return getReactiveConnection(session).select( sql, params )
//				.thenApply( resultSet -> !resultSet.hasNext() );
//	}

	default CompletionStage<Object[]> reactiveGetDatabaseSnapshot(Serializable id,
																  SharedSessionContractImplementor session) {
		if ( log.isTraceEnabled() ) {
			log.tracev(
					"Getting current persistent state for: {0}",
					infoString( this, id, getFactory() )
			);
		}

		Object[] params = PreparedStatementAdaptor.bind(
				statement -> getIdentifierType().nullSafeSet(statement, id, 1, session)
		);

		return getReactiveConnection( session )
				.selectJdbc( delegate().getSQLSnapshotSelectString(), params )
				.thenApply( (resultSet) -> processSnapshot(session, resultSet) );
	}

	//would be nice of we could just reuse this code from AbstractEntityPersister
	default Object[] processSnapshot(SharedSessionContractImplementor session, ResultSet resultSet) {
		try {
			if ( resultSet.next() ) {
				//return the "hydrated" state (ie. associations are not resolved)
				Type[] types = getPropertyTypes();
				Object[] values = new Object[types.length];
				boolean[] includeProperty = getPropertyUpdateability();
				for ( int i = 0; i < types.length; i++ ) {
					if ( includeProperty[i] ) {
						values[i] = types[i].hydrate(
								resultSet,
								getPropertyAliases( "", i ),
								session,
								null
						); //null owner ok??
					}
				}
				return values;
			}
			else {
				//no corresponding row: transient!
				return null;
			}
		}
		catch (SQLException e) {
			//can't actually occur!
			throw new JDBCException( "error while binding parameters", e );
		}
	}

	default boolean hasUnenhancedProxy() {
		// skip proxy instantiation if entity is bytecode enhanced
		return getEntityMetamodel().isLazy()
				&& !( getEntityMetamodel().getBytecodeEnhancementMetadata().isEnhancedForLazyLoading()
					&& getFactory().getSessionFactoryOptions().isEnhancementAsProxyEnabled() );
	}

	Object initializeLazyProperty(String fieldName, Object entity, SharedSessionContractImplementor session);

	@SuppressWarnings("unchecked")
	default <E,T> CompletionStage<T> reactiveInitializeLazyProperty(Attribute<E,T> field, E entity,
																	SharedSessionContractImplementor session) {
		Object result = initializeLazyProperty( field.getName(), entity, session );
		if (result instanceof CompletionStage) {
			return (CompletionStage<T>) result;
		}
		else {
			return CompletionStages.nullFuture();
		}
	}

	default CompletionStage<?> reactiveInitializeLazyPropertiesFromDatastore(
			final String fieldName,
			final Object entity,
			final SharedSessionContractImplementor session,
			final Serializable id,
			final EntityEntry entry) {

		if ( !hasLazyProperties() ) {
			throw new AssertionFailure( "no lazy properties" );
		}

		final PersistentAttributeInterceptor interceptor = ( (PersistentAttributeInterceptable) entity ).$$_hibernate_getInterceptor();
		assert interceptor != null : "Expecting bytecode interceptor to be non-null";

		log.tracef( "Initializing lazy properties from datastore (triggered for `%s`)", fieldName );

		final String fetchGroup = getEntityMetamodel().getBytecodeEnhancementMetadata()
				.getLazyAttributesMetadata()
				.getFetchGroupName( fieldName );
		final List<LazyAttributeDescriptor> fetchGroupAttributeDescriptors = getEntityMetamodel().getBytecodeEnhancementMetadata()
				.getLazyAttributesMetadata()
				.getFetchGroupAttributeDescriptors( fetchGroup );

		@SuppressWarnings("deprecation")
		final Set<String> initializedLazyAttributeNames = interceptor.getInitializedLazyAttributeNames();

		Object[] params = PreparedStatementAdaptor.bind(
				statement -> getIdentifierType().nullSafeSet( statement, id, 1, session )
		);

		String lazySelect = getSQLLazySelectString( fetchGroup );

		// null sql means that the only lazy properties
		// are shared PK one-to-one associations which are
		// handled differently in the Type#nullSafeGet code...
		if ( lazySelect == null ) {
			return CompletionStages.completedFuture( initLazyProperty(
					fieldName, entity,
					session, entry,
					interceptor,
					fetchGroupAttributeDescriptors,
					initializedLazyAttributeNames,
					null
			) );
		}

		return ((ReactiveSession) session).getReactiveConnection()
				.selectJdbc( lazySelect, params )
				.thenApply( resultSet -> {
					try {
						resultSet.next();
						return initLazyProperty(
								fieldName, entity,
								session, entry,
								interceptor,
								fetchGroupAttributeDescriptors,
								initializedLazyAttributeNames,
								resultSet
						);
					}
					catch (SQLException sqle) {
						//can't occur
						throw new JDBCException("error initializing lazy property", sqle);
					}
				} );
	}

	default Object initLazyProperty(String fieldName, Object entity, SharedSessionContractImplementor session, EntityEntry entry, PersistentAttributeInterceptor interceptor, List<LazyAttributeDescriptor> fetchGroupAttributeDescriptors, Set<String> initializedLazyAttributeNames, ResultSet rs)  {
		for ( LazyAttributeDescriptor fetchGroupAttributeDescriptor: fetchGroupAttributeDescriptors ) {
			final boolean previousInitialized =
					initializedLazyAttributeNames.contains( fetchGroupAttributeDescriptor.getName() );

			if ( previousInitialized ) {
				continue;
			}

			final Object selectedValue;
			try {
				selectedValue = fetchGroupAttributeDescriptor.getType().nullSafeGet(
						rs,
						getLazyPropertyColumnAliases()[ fetchGroupAttributeDescriptor.getLazyIndex() ],
						session,
						entity
				);
			}
			catch (SQLException sqle) {
				//can't occur
				throw new JDBCException("error initializing lazy property", sqle);
			}

			final boolean set = initializeLazyProperty(
					fieldName,
					entity,
					session,
					entry,
					fetchGroupAttributeDescriptor.getLazyIndex(),
					selectedValue
			);

			if ( set ) {
				interceptor.attributeInitialized( fetchGroupAttributeDescriptor.getName() );
			}

			log.trace( "Done initializing lazy properties" );

			return selectedValue;
		}

		return null;
	}

	boolean initializeLazyProperty(String fieldName,
								   Object entity,
								   SharedSessionContractImplementor session,
								   EntityEntry entry,
								   int lazyIndex,
								   Object selectedValue);

	String[][] getLazyPropertyColumnAliases();

	String getSQLLazySelectString(String fetchGroup);

	boolean isBatchable();

	class UpdateExpectation implements ReactiveConnection.Expectation {
		private boolean successful;

		private final Serializable id;
		private final Expectation expectation;
		private final ReactiveAbstractEntityPersister persister;

		public UpdateExpectation(Serializable id, Expectation expectation, ReactiveAbstractEntityPersister persister) {
			this.id = id;
			this.expectation = expectation;
			this.persister = persister;
		}

		@Override
		public void verifyOutcome(int rowCount, int batchPosition, String batchSql) {
			successful = persister.check( rowCount, id, batchPosition, expectation, new PreparedStatementAdaptor(), batchSql );
		}

		public boolean isSuccessful() {
			return successful;
		}
	}

	class DeleteExpectation implements ReactiveConnection.Expectation {
		private final Serializable id;
		private final Expectation expectation;
		private final ReactiveAbstractEntityPersister persister;

		public DeleteExpectation(Serializable id, Expectation expectation, ReactiveAbstractEntityPersister persister) {
			this.id = id;
			this.expectation = expectation;
			this.persister = persister;
		}

		@Override
		public void verifyOutcome(int rowCount, int batchPosition, String batchSql) {
			persister.check( rowCount, id, batchPosition, expectation, null, batchSql );
		}
	}

	class InsertExpectation implements ReactiveConnection.Expectation {
		private final Expectation expectation;
		private ReactiveAbstractEntityPersister persister;

		public InsertExpectation(Expectation expectation, ReactiveAbstractEntityPersister persister) {
			this.expectation = expectation;
			this.persister = persister;
		}

		@Override
		public void verifyOutcome(int rowCount, int batchPosition, String batchSql) {
			try {
				expectation.verifyOutcome( rowCount, null, batchPosition, batchSql );
			} catch (SQLException e) {
				//can't actually occur!
				throw new JDBCException("error while verifying result count", e);
			}
		}
	}
}
