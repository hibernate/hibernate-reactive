package org.hibernate.rx.persister.entity.impl;

import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.Session;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL81Dialect;
import org.hibernate.engine.OptimisticLockStyle;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.spi.*;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.jdbc.Expectation;
import org.hibernate.jdbc.Expectations;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.JoinedSubclassEntityPersister;
import org.hibernate.persister.entity.MultiLoadOptions;
import org.hibernate.persister.entity.OuterJoinLoadable;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.loader.entity.impl.RxDynamicBatchingEntityLoaderBuilder;
import org.hibernate.rx.sql.impl.Parameters;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.sql.Delete;
import org.hibernate.tuple.InMemoryValueGenerationStrategy;
import org.hibernate.tuple.entity.EntityTuplizer;
import org.hibernate.type.Type;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * An abstract implementation of {@link RxEntityPersister} whose
 * concrete implementations each extend a concrete subclass of
 * {@link AbstractEntityPersister}. Note that there are three
 * main flavors of {@code AbstractEntityPersister}, one for each
 * of the three inheritance mapping strategies, and thus we have
 * three flavors of {@link RxEntityPersister}. Therefore, this
 * interface is defined as a mixin. This design avoid duplicating
 * the code in this class in the three different subclasses.
 *
 * Concrete implementations of this interface _must_ also extend
 * {@code AbstractEntityPersister} or one of its concrete
 * subclasses.
 *
 * @see RxJoinedSubclassEntityPersister
 * @see RxUnionSubclassEntityPersister
 * @see RxSingleTableEntityPersister
 */
public interface RxAbstractEntityPersister extends RxEntityPersister, OuterJoinLoadable {
	Logger log = Logger.getLogger( JoinedSubclassEntityPersister.class );

	RxQueryExecutor queryExecutor = new RxQueryExecutor();

	/**
	 * This is a copy of a
	 * {@link AbstractEntityPersister#preInsertInMemoryValueGeneration(Object[], Object, SharedSessionContractImplementor) private method}
	 * of {@code AbstractEntityPersister} that we could have made public.
	 */
	static void preInsertInMemoryValueGeneration(
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session,
			AbstractEntityPersister delegate) {
		if ( delegate.getEntityMetamodel().hasPreInsertGeneratedValues() ) {
			final InMemoryValueGenerationStrategy[] strategies = delegate.getEntityMetamodel().getInMemoryValueGenerationStrategies();
			for ( int i = 0; i < strategies.length; i++ ) {
				if ( strategies[i] != null && strategies[i].getGenerationTiming().includesInsert() ) {
					fields[i] = strategies[i].getValueGenerator().generateValue( (Session) session, object );
					delegate.setPropertyValue( object, i, fields[i] );
				}
			}
		}
	}

	default RxQueryExecutor queryExecutor() {
		return queryExecutor;
	}

	/**
	 * A self-reference of type {@code AbstractEntityPersister}.
	 *
	 * @return this object
	 */
	default AbstractEntityPersister delegate() {
		return (AbstractEntityPersister) this;
	}

	@Override
	default CompletionStage<Serializable> insertRx(Object[] fields, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session, delegate() );

		final int span = delegate().getTableSpan();
		CompletionStage<Serializable> stage = RxUtil.completedFuture(null);
		if ( delegate().getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = delegate().getPropertiesToInsert( fields );
			stage = stage.thenCompose( n -> insertRx( fields, notNull, delegate().generateInsertString( true, notNull ), object, session ) );
			for ( int j = 1; j < span; j++ ) {
				final int jj = j;
				stage = stage.thenCompose( id ->
						insertRx(id, fields, notNull, jj, delegate().generateInsertString(notNull, jj), object, session)
							.thenApply( v -> id ));
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			stage = stage.thenCompose( n -> insertRx( fields, delegate().getPropertyInsertability(), delegate().getSQLIdentityInsertString(), object, session ) );
			for ( int j = 1; j < span; j++ ) {
				final int jj = j;
				stage = stage.thenCompose( id ->
						insertRx(id, fields, delegate().getPropertyInsertability(), jj, delegate().getSQLInsertStrings()[jj], object, session)
								.thenApply( v -> id ));
			}
		}
		return stage;
	}

	@Override
	default CompletionStage<?> insertRx(
			Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session) {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session, delegate() );

		CompletionStage<?> insertStage = RxUtil.nullFuture();
		final int span = delegate().getTableSpan();
		if ( delegate().getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = delegate().getPropertiesToInsert( fields );
			for ( int j = 0; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose(
						v -> insertRx(
								id,
								fields,
								notNull,
								jj,
								delegate().generateInsertString( notNull, jj ),
								object,
								session
						));
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			for ( int j = 0; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose(
						v -> insertRx(
								id,
								fields,
								delegate().getPropertyInsertability(),
								jj,
								delegate().getSQLInsertStrings()[jj],
								object,
								session
						));
			}
		}
		return insertStage;
	}

	default CompletionStage<?> insertRx(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {

		if ( delegate().isInverseTable( j ) ) {
			return RxUtil.nullFuture();
		}

		//note: it is conceptually possible that a UserType could map null to
		//	  a non-null value, so the following is arguable:
		if ( delegate().isNullableTable( j ) && delegate().isAllNull( fields, j ) ) {
			return RxUtil.nullFuture();
		}

		if ( log.isTraceEnabled() ) {
			log.tracev( "Inserting entity: {0}", MessageHelper.infoString(delegate(), id, delegate().getFactory() ) );
			if ( j == 0 && delegate().isVersioned() ) {
				log.tracev( "Version: {0}", Versioning.getVersion( fields, delegate()) );
			}
		}

		// TODO : shouldn't inserts be Expectations.NONE?
		final Expectation expectation = Expectations.appropriateExpectation( delegate().getInsertResultCheckStyles()[j] );
//		final int jdbcBatchSizeToUse = session.getConfiguredJdbcBatchSize();
//		final boolean useBatch = expectation.canBeBatched() &&
//				jdbcBatchSizeToUse > 1 &&
//				delegate.getIdentifierGenerator().supportsJdbcBatchInserts();

//		if ( useBatch && insertBatchKey == null ) {
//			insertBatchKey = new BasicBatchKey(
//					delegate.getEntityName() + "#INSERT",
//					expectation
//			);
//		}
//		final boolean callable = delegate.isInsertCallable( j );

		PreparedStatementAdaptor insert = new PreparedStatementAdaptor();
		try {
			int index = delegate().dehydrate( null, fields, notNull, delegate().getPropertyColumnInsertable(), j, insert, session, false );
			delegate().getIdentifierType().nullSafeSet( insert, id, index, session );
		}
		catch (SQLException e) {
			//can't actually occur!
			throw new JDBCException( "error while binding parameters", e );
		}
		return queryExecutor().update( sql, insert.getParametersAsArray(), delegate().getFactory() )
				.thenAccept( count -> {
					try {
						expectation.verifyOutcome(count, insert, -1);
					}
					catch (SQLException e) {
						//can't actually occur!
						throw new JDBCException( "error while verifying result count", e );
					}
				});
	}

	/**
	 * Perform an SQL INSERT, and then retrieve a generated identifier.
	 * <p/>
	 * This form is used for PostInsertIdentifierGenerator-style ids (IDENTITY,
	 * select, etc).
	 */
	default CompletionStage<Serializable> insertRx(
			Object[] fields,
			boolean[] notNull,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {

		sql = Parameters.processParameters(sql, session);

		if ( log.isTraceEnabled() ) {
			log.tracev( "Inserting entity: {0}", MessageHelper.infoString(delegate()) );
			if ( delegate().isVersioned() ) {
				log.tracev( "Version: {0}", Versioning.getVersion( fields, delegate()) );
			}
		}

		PreparedStatementAdaptor insert = new PreparedStatementAdaptor();
		try {
			delegate().dehydrate( null, fields, notNull, delegate().getPropertyColumnInsertable(), 0, insert, session, false );
		}
		catch (SQLException e) {
			//can't actually occur!
			throw new JDBCException( "error while binding parameters", e );
		}

		SessionFactoryImplementor factory = session.getFactory();
		Dialect dialect = factory.getJdbcServices().getDialect();
		String identifierColumnName = delegate().getIdentifierColumnNames()[0];
		if ( factory.getSessionFactoryOptions().isGetGeneratedKeysEnabled() ) {
			//TODO: wooooo this is awful ... I believe the problem is fixed in Hibernate 6
			if ( dialect instanceof PostgreSQL81Dialect) {
				sql = sql + " returning " + identifierColumnName;
			}
			return queryExecutor().updateReturning( sql, insert.getParametersAsArray(), factory )
					.thenApply(Optional::get);
		}
		else {
			//use an extra round trip to fetch the id
			String selectIdSql = dialect.getIdentityColumnSupport()
					.getIdentitySelectString(
							delegate().getTableName(),
							identifierColumnName,
							Types.INTEGER
					);
			return queryExecutor().update( sql, insert.getParametersAsArray(), factory)
					.thenCompose( v -> queryExecutor().selectLong(selectIdSql, new Object[0], factory) )
					.thenApply(Optional::get);
		}

	}

	default CompletionStage<?> deleteRx(
			Serializable id,
			Object version,
			int j,
			Object object,
			String sql,
			SharedSessionContractImplementor session,
			Object[] loadedState) throws HibernateException {

		if ( delegate().isInverseTable( j ) ) {
			return RxUtil.nullFuture();
		}
		final boolean useVersion = j == 0 && delegate().isVersioned();
//		final boolean callable = delegate.isDeleteCallable( j );
		final Expectation expectation = Expectations.appropriateExpectation( delegate().getDeleteResultCheckStyles()[j] );
//		final boolean useBatch = j == 0 && delegate.isBatchable() && expectation.canBeBatched();
//		if ( useBatch && deleteBatchKey == null ) {
//			deleteBatchKey = new BasicBatchKey(
//					delegate.getEntityName() + "#DELETE",
//					expectation
//			);
//		}

		if ( log.isTraceEnabled() ) {
			log.tracev( "Deleting entity: {0}", MessageHelper.infoString(delegate(), id, delegate().getFactory() ) );
			if ( useVersion ) {
				log.tracev( "Version: {0}", version );
			}
		}

		if ( delegate().isTableCascadeDeleteEnabled( j ) ) {
			if ( log.isTraceEnabled() ) {
				log.tracev( "Delete handled by foreign key constraint: {0}", delegate().getTableName( j ) );
			}
			//EARLY EXIT!
			return RxUtil.nullFuture();
		}

		//Render the SQL query
		PreparedStatementAdaptor delete = new PreparedStatementAdaptor();
		try {
			// FIXME: This is a hack to set the right type for the parameters
			//		until we have a proper type system in place
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
		}
		catch ( SQLException e) {
			throw new HibernateException( e );
		}

		return queryExecutor().update( sql, delete.getParametersAsArray(), delegate().getFactory() )
				.thenAccept( count -> {
					try {
						expectation.verifyOutcome(count, delete, -1);
					}
					catch (SQLException e) {
						//can't actually occur!
						throw new JDBCException( "error while verifying result count", e );
					}
				});
	}

	default CompletionStage<?> deleteRx(
			Serializable id, Object version, Object object,
			SharedSessionContractImplementor session)
			throws HibernateException {
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

		CompletionStage<?> deleteStage = RxUtil.nullFuture();
		for ( int j = span - 1; j >= 0; j-- ) {
			// For now we assume there is only one delete query
			int jj = j;
			Object[] state = loadedState;
			deleteStage = deleteStage.thenCompose(
					v-> deleteRx(
							id,
							version,
							jj,
							object,
							deleteStrings[jj],
							session,
							state
					));
		}

		return deleteStage;
	}

	/**
	 * This is a copy of a
	 * {@link AbstractEntityPersister#isAllOrDirtyOptLocking() private method}
	 * of {@code AbstractEntityPersister} that we could have made public.
	 */
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

	default CompletionStage<Boolean> updateRx(
			final Serializable id,
			final Object[] fields,
			final Object[] oldFields,
			final Object rowId,
			final boolean[] includeProperty,
			final int j,
			final Object oldVersion,
			final Object object,
			final String sql,
			final SharedSessionContractImplementor session) throws HibernateException {

		final Expectation expectation = Expectations.appropriateExpectation( delegate().getUpdateResultCheckStyles()[j] );
//		final int jdbcBatchSizeToUse = session.getConfiguredJdbcBatchSize();
//		final boolean useBatch = expectation.canBeBatched() && isBatchable() && jdbcBatchSizeToUse > 1;
//		if ( useBatch && updateBatchKey == null ) {
//			updateBatchKey = new BasicBatchKey(
//					delegate.getEntityName() + "#UPDATE",
//					expectation
//			);
//		}
//		final boolean callable = delegate.isUpdateCallable( j );
		final boolean useVersion = j == 0 && delegate().isVersioned();

		if ( log.isTraceEnabled() ) {
			log.tracev( "Updating entity: {0}", MessageHelper.infoString(delegate(), id, delegate().getFactory() ) );
			if ( useVersion ) {
				log.tracev( "Existing version: {0} -> New version:{1}", oldVersion, fields[delegate().getVersionProperty()] );
			}
		}

		try {
			int index = 1; // starting index
//			if ( useBatch ) {
//				update = session
//						.getJdbcCoordinator()
//						.getBatch( updateBatchKey )
//						.getBatchStatement( sql, callable );
//			}
//			else {
			final PreparedStatementAdaptor update = new PreparedStatementAdaptor();
//			}

			try {
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

//				if ( useBatch ) {
//					session.getJdbcCoordinator().getBatch( updateBatchKey ).addToBatch();
//					return true;
//				}
//				else {
				return queryExecutor().update( sql, update.getParametersAsArray(), delegate().getFactory() )
						.thenApply( count -> {
							try {
								expectation.verifyOutcome(count, update, -1);
							}
							catch (SQLException e) {
								//can't actually occur!
								throw new JDBCException( "error while verifying result count", e );
							}
							return count > 0;
						});
//					return check(
//							session.getJdbcCoordinator().getResultSetReturn().executeUpdate( update ),
//							id,
//							j,
//							expectation,
//							update
//					);
//				}
			}
			finally {
//				if ( !useBatch ) {
//					session.getJdbcCoordinator().getResourceRegistry().release( update );
//					session.getJdbcCoordinator().afterStatementExecution();
//				}
			}

		}
		catch (SQLException e) {
			throw delegate().getFactory().getSQLExceptionHelper().convert(
					e,
					"could not update: " + MessageHelper.infoString(delegate(), id, delegate().getFactory() ),
					sql
			);
		}
	}

	default CompletionStage<?> updateRx(
			final Serializable id,
			final Object[] fields,
			int[] dirtyFields,
			final boolean hasDirtyCollection,
			final Object[] oldFields,
			final Object oldVersion,
			final Object object,
			final Object rowId,
			final SharedSessionContractImplementor session) throws HibernateException {

		// apply any pre-update in-memory value generation
		if ( delegate().getEntityMetamodel().hasPreUpdateGeneratedValues() ) {
			final InMemoryValueGenerationStrategy[] valueGenerationStrategies = delegate().getEntityMetamodel().getInMemoryValueGenerationStrategies();
			int valueGenerationStrategiesSize = valueGenerationStrategies.length;
			if ( valueGenerationStrategiesSize != 0 ) {
				int[] fieldsPreUpdateNeeded = new int[valueGenerationStrategiesSize];
				int count = 0;
				for ( int i = 0; i < valueGenerationStrategiesSize; i++ ) {
					if ( valueGenerationStrategies[i] != null && valueGenerationStrategies[i].getGenerationTiming()
							.includesUpdate() ) {
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
			updateStrings = getSQLUpdateStrings(
					rowId != null,
					delegate().hasUninitializedLazyProperties( object )
			);
			propsToUpdate = delegate().getPropertyUpdateability( object );
		}

		CompletionStage<?> updateStage = RxUtil.nullFuture();
		for ( int j = 0; j < span; j++ ) {
			// Now update only the tables with dirty properties (and the table with the version number)
			if ( tableUpdateNeeded[j] ) {
				// We assume there is only one table for now
				final int jj = j;
				updateStage = updateStage.thenCompose(
						v -> updateOrInsertRx(
								id,
								fields,
								oldFields,
								jj == 0 ? rowId : null,
								propsToUpdate,
								jj,
								oldVersion,
								object,
								updateStrings[jj],
								session
						));
			}
		}
		return updateStage;
	}

	default CompletionStage<?> updateOrInsertRx(
			final Serializable id,
			final Object[] fields,
			final Object[] oldFields,
			final Object rowId,
			final boolean[] includeProperty,
			final int j,
			final Object oldVersion,
			final Object object,
			final String sql,
			final SharedSessionContractImplementor session) throws HibernateException {

		if ( !delegate().isInverseTable( j ) ) {

			if ( delegate().isNullableTable( j ) && delegate().isAllNull( oldFields, j ) && oldFields != null ) {
				// don't bother trying to update, we know there is no row there yet
				if ( !delegate().isAllNull( fields, j ) ) {
					return insertRx( id, fields, delegate().getPropertyInsertability(), j, delegate().getSQLInsertStrings()[j], object, session );
				}
			}
			else if ( delegate().isNullableTable( j ) && delegate().isAllNull( fields, j ) ) {
				// All fields are null, we can just delete the row
				return deleteRx( id, oldVersion, j, object, delegate().getSQLDeleteStrings()[j], session, null );
			}
			else {
				return updateRx( id, fields, oldFields, rowId, includeProperty, j, oldVersion, object, sql, session )
						.thenApply( updated -> {
							if ( !updated && !delegate().isAllNull( fields, j ) ) {
								// Nothing has been updated because the row isn't in the db
								// Run an insert instead
								return insertRx( id, fields, delegate().getPropertyInsertability(), j, delegate().getSQLInsertStrings()[j], object, session );
							}
							return null;
						} );
			}
		}

		// Nothing to do;
		return RxUtil.nullFuture();
	}

	/**
	 * This is a copy of a
	 * {@link AbstractEntityPersister#getUpdateStrings(boolean, boolean) private method}
	 * of {@code AbstractEntityPersister} that we could have made public.
	 */
	default String[] getSQLUpdateStrings(boolean byRowId, boolean lazy) {
		return byRowId ?
				lazy ? delegate().getSQLLazyUpdateByRowIdStrings() : delegate().getSQLUpdateByRowIdStrings() :
				lazy ? delegate().getSQLLazyUpdateStrings() : delegate().getSQLUpdateStrings();
	}

	@Override
	default CompletionStage<List<?>> rxMultiLoad(Serializable[] ids, SessionImplementor session, MultiLoadOptions loadOptions) {
		return RxDynamicBatchingEntityLoaderBuilder.INSTANCE.multiLoad(this, ids, session, loadOptions);
	}

	/**
	 * @deprecated use {@link #setRxPropertyValues(Object, Object[])} instead
	 */
	@Override
	@Deprecated
	default void setPropertyValues(Object object, Object[] values) {
		throw  new UnsupportedOperationException( "Use setRxPropertyValues instead");
	}

	default CompletionStage<Void> setRxPropertyValues(Object object, Object[] values) {
		EntityTuplizer tuplizer = getEntityMetamodel().getTuplizer();
		CompletionStage<Void> stage = RxUtil.nullFuture();
		for (int i = 0; i < values.length; i++) {
			if ( values[i] instanceof  CompletionStage ) {
				int index = i;
				final CompletionStage<?> valueStage = (CompletionStage<?>) values[i];
				stage = stage.thenCompose( v -> valueStage.thenAccept( obj -> {
					values[index] = read(obj);
				}));
			}
		}
		return stage.thenAccept( v ->  tuplizer.setPropertyValues( object, values ) );
	}

	default Object read(Object obj) {
		return obj instanceof Optional
				? ( (Optional) obj ).orElse( null )
				: obj;
	}
}
