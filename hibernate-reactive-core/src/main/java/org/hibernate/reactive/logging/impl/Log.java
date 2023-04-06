/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.logging.impl;



import java.sql.SQLWarning;

import jakarta.persistence.PersistenceException;

import org.hibernate.HibernateException;
import org.hibernate.LazyInitializationException;
import org.hibernate.cache.CacheException;
import org.hibernate.id.IdentifierGenerationException;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.FormatWith;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

@MessageLogger(projectCode = "HR")
public interface Log extends BasicLogger {

	@LogMessage(level = INFO)
	@Message(id = 1, value = "Hibernate Reactive")
	void startHibernateReactive();

	@LogMessage(level = INFO)
	@Message(id = 2, value = "Vert.x not detected, creating a new instance")
	void creatingVertxInstance();

	@LogMessage(level = INFO)
	@Message(id = 3, value = "Vert.x instance stopped")
	void vertxInstanceStopped();

	@LogMessage(level = INFO)
	@Message(id = 11, value = "SQL Client URL [%1$s]")
	void sqlClientUrl(String url);

	@LogMessage(level = INFO)
	@Message(id = 13, value = "Detected driver [%1$s] %2$s")
	void detectedDriver(String driverName, String checkmark);

	@LogMessage(level = INFO)
	@Message(id = 14, value = "Prepared statement cache disabled")
	void preparedStatementCacheDisabled();

	@LogMessage(level = INFO)
	@Message(id = 15, value = "Prepared statement cache max size: %1$d")
	void preparedStatementCacheMaxSize(Comparable<Integer> cacheMaxSize);

	@LogMessage(level = INFO)
	@Message(id = 16, value = "Prepared statement cache SQL limit: %1$d")
	void preparedStatementCacheSQLLimit(Integer sqlLimit);

	@LogMessage(level = INFO)
	@Message(id = 17, value = "Using SQL client configuration [%1$s]")
	void sqlClientConfiguration(String configClassName);

	@LogMessage(level = INFO)
	@Message(id = 18, value = "Instantiating reactive pool: %1$s")
	void instantiatingReactivePool(@FormatWith(ClassFormatter.class) Class<?> implClass);

	@LogMessage(level = WARN)
	@Message(id = 21, value = "DDL command failed [%1$s]")
	void ddlCommandFailed(String message);

	@LogMessage(level = INFO)
	@Message(id = 25, value = "Connection pool size: %1$d")
	void connectionPoolSize(int poolSize);

	@LogMessage(level = INFO)
	@Message(id = 26, value = "Connection pool max wait queue size: %1$d")
	void connectionPoolMaxWaitSize(Integer maxWaitQueueSize);

	@LogMessage(level = INFO)
	@Message(id = 27, value = "Connection pool idle timeout: %1$d ms")
	void connectionPoolIdleTimeout(Integer idleTimeout);

	@LogMessage(level = INFO)
	@Message(id = 28, value = "Connection pool connection timeout: %1$d ms")
	void connectionPoolTimeout(Integer connectTimeout);

	@LogMessage(level = INFO)
	@Message(id = 29, value = "Connection pool cleaner period: %1$d ms")
	void connectionPoolCleanerPeriod(Integer poolCleanerPeriod);

	@Message(id = 31, value = "More than one row with the given identifier was found: %1$s, for class: %2$s")
	HibernateException moreThanOneRowWithTheGivenIdentifier(Object id, String entityName);

	@Message(id = 32, value = "Could not instantiate SQL client pool configuration [%1$s]")
	HibernateException couldNotInstantiatePoolConfiguration(String configClassName, @Cause Throwable error);

	@Message(id = 33, value = "Unable to locate row for retrieval of generated properties: %1$s")
	HibernateException unableToRetrieveGeneratedProperties(String infoString);

	@Message(id = 34, value = "The database returned no natively generated identity value")
	HibernateException noNativelyGeneratedValueReturned();

	@Message(id = 35, value = "The database can only generate identifiers of type Long")
	HibernateException nativelyGeneratedValueMustBeLong();

	@Message(id = 356, value = "Wrong entity type!")
	HibernateException wrongEntityType();

	@Message(id = 36, value = "firstResult/maxResults specified with collection fetch. In memory pagination was about to be applied. Failing because 'Fail on pagination over collection fetch' is enabled.")
	HibernateException firstOrMaxResultsFailedBecausePaginationOverCollectionIsEnabled();

	@Message(id = 37, value = "Reactive sessions do not support transparent lazy fetching - use Session.fetch() (entity '%1$s' with id '%2$s' was not loaded)")
	LazyInitializationException lazyInitializationException(String entityName, Object id);

	@Message(id = 38, value = "Entity [%1$s] did not define a natural id")
	HibernateException entityDidNotDefinedNaturalId(String entityName);

	@Message(id = 39, value = "Flush during cascade is dangerous" )
	HibernateException flushDuringCascadeIsDangerous();

	@Message(id = 40, value = "An immutable natural identifier of entity %1$s was altered from %2$s to %3$s" )
	HibernateException immutableNaturalIdentifierAltered(String entityName, String from, String to);

	@Message(id = 41, value = "Identifier of an instance of %1$s was altered from %2$s to %3$s")
	HibernateException identifierAltered(String entityName, Object id, Object oid);

	@Message(id = 42, value = "Merge requested with id not matching id of passed entity")
	HibernateException mergeRequestedIdNotMatchingIdOfPassedEntity();

	@Message(id = 43, value = "Unable to locate persister: %1$s")
	HibernateException unableToLocatePersister(String persister);

	@Message(id = 44, value = "Invalid lock mode for lock()")
	HibernateException invalidLockModeForLock();

	@Message(id = 45, value = "Collection was evicted")
	HibernateException collectionWasEvicted();

	@Message(id = 46, value = "Unexpected batch size spread")
	HibernateException unexpectedBatchSizeSpread();

	@Message(id = 47, value = "Generated identifier smaller or equal to 0: %1$d")
	HibernateException generatedIdentifierSmallerOrEqualThanZero(Long id);

	@Message(id = 48, value = "Could not determine Dialect from JDBC driver metadata (specify a connection URI with scheme 'postgresql:', 'mysql:', 'cockroachdb', or 'db2:')")
	HibernateException couldNotDetermineDialectFromJdbcDriverMetadata();

	@Message(id = 49, value = "Could not determine Dialect from connection URI: '%1$s' (specify a connection URI with scheme 'postgresql:', 'mysql:', 'cockroachdb', or 'db2:')")
	HibernateException couldNotDetermineDialectFromConnectionURI(String url);

	@Message(id = 50, value = "SelectGenerator is not supported in Hibernate Reactive")
	HibernateException selectGeneratorIsNotSupportedInHibernateReactive();

	@Message(id = 51, value = "Cannot generate identifiers of type %1$S for: %2$s")
	HibernateException cannotGenerateIdentifiersOfType(String simpleName, String entityName);

	@Message(id = 52, value = "Generated identifier for %1$s too big to be assigned to a field of type %2$s: %3$s")
	HibernateException generatedIdentifierTooBigForTheField(String entityName, String simpleName, Long id);

	@Message(id = 53, value = "Could not locate EntityEntry immediately after two-phase load")
	HibernateException couldNotLocateEntityEntryAfterTwoPhaseLoad();

	@Message(id = 54, value = "Cannot recreate collection while filter is enabled: %1$s")
	HibernateException cannotRecreateCollectionWhileFilterIsEnabled(String collectionInfoString);

	@Message(id = 55, value = "Session closed")
	LazyInitializationException sessionClosedLazyInitializationException();

	@Message(id = 56, value = "Collection cannot be initialized: %1$s - Fetch the collection using 'Mutiny.fetch', 'Stage.fetch', or 'fetch join' in HQL")
	LazyInitializationException collectionCannotBeInitializedlazyInitializationException(String role);

	@LogMessage(level = ERROR)
	@Message(id = 57, value = "Failed to execute statement [%1$s]: %2$s")
	void failedToExecuteStatement(String sql, String s, @Cause Throwable t);

	@Message(id = 58, value = "Impossible flush mode")
	IllegalStateException impossibleFlushModeIllegalState();

	@Message(id = 59, value = "Number of results is greater than number of batched parameters")
	IllegalStateException numberOfResultsGreaterThanBatchedParameters();

	@Message(id = 60, value = "Session is closed")
	IllegalStateException sessionIsClosed();

	@Message(id = 61, value = "Session is currently connecting to database")
	IllegalStateException sessionIsConnectingToTheDatabase();

	@Message(id = 62, value = "Updating immutable entity that is not in session yet!")
	IllegalStateException updatingImmutableEntityThatsNotInTheSession();

	@Message(id = 63, value = "Entity has status Status.DELETED but values != entry.getDeletedState")
	IllegalStateException entityDeleteStateIllegal();

	@Message(id = 64, value = "Number of return values [%1$s] did not match expected [%2$s]")
	IllegalStateException unexpectedNumberOfReturnedValues(int length, int size);

	@Message(id = 65, value = "No Vert.x context active")
	IllegalStateException notVertxContextActive();

	@Message(id = 66, value = "Unknown structure type")
	IllegalStateException unknownStructureType();

	@Message(id = 67, value= "Service not initialized")
	IllegalStateException serviceNotInitialized();

	@Message(id = 68, value = "This method should exclusively be invoked from a Vert.x EventLoop thread; currently running on thread '%1$s'")
	IllegalStateException shouldBeInvokedInVertxEventLoopThread(String name);

	@Message(id = 69, value = "Detected use of the reactive Session from a different Thread than the one which was used to open the reactive Session - this suggests an invalid integration; "
			+ "original thread [%1$s]: '%2$s' current Thread [%3$s]: '%4$s'")
	IllegalStateException detectedUsedOfTheSessionOnTheWrongThread(long expectedThreadId, String expectedThreadName, long currentThreadId, String currentThreadName);

	@Message(id = 70, value = "Unable to locate persistence units")
	PersistenceException unableToLocatePersistenceUnits(@Cause Throwable e);

	@Message(id = 71, value= "No name provided and multiple persistence units found")
	PersistenceException noNameProvidedAndMultiplePersistenceUnitsFound();

	@Message(id = 72, value= "Cannot update an uninitialized proxy. Make sure to fetch the value before trying to update it: %1$s")
	HibernateException uninitializedProxyUpdate(Object entity);

	@Message(id = 73, value = "%1$s is an invalid identity type when using CockroachDB (entity %2$s) - CockroachDB might generates identifiers that are too big and won't always fit in a %1$s. java.lang.Long is valid replacement")
	HibernateException invalidIdentifierTypeForCockroachDB(@FormatWith(ClassFormatter.class) Class<?> idType, String entityName);

	@Message(id = 74, value = "ids for this class must be manually assigned before calling save(): %1$s")
	IdentifierGenerationException idMustBeAssignedBeforeSave(String entityName);

	@Message(id = 75, value = "Detected call to non reactive method. Alternative reactive method: `%1$s`")
	HibernateException nonReactiveMethodCall(String alternativeMethod);

	// A helper to keep track of places that still need to be done
	@Message(id = 76, value = "This method has not been implemented yet")
	HibernateException notYetImplemented();

	@Message(id = 77, value = "Unable to execute post-insert id selection query: %1$s")
	HibernateException unableToExecutePostInsertIdSelectionQuery(String selectSQL, @Cause Throwable e);

	@Message(id = 78, value = "Unable to bind parameters for post-insert id selection query: %1$s")
	HibernateException bindParametersForPostInsertIdSelectQueryError(String selectSQL, @Cause Throwable e);

	// Same method that exists in CoreMessageLogger
	@LogMessage(level = WARN)
	@Message(id = 104, value = "firstResult/maxResults specified with collection fetch; applying in memory!" )
	void firstOrMaxResultsSpecifiedWithCollectionFetch();

	// Same method that exists in CoreMessageLogger
	@LogMessage(level = INFO)
	@Message(id = 114, value = "Handling transient entity in delete processing" )
	void handlingTransientEntity();

	// Same method that exists in CoreMessageLogger
	@LogMessage(level = WARN)
	@Message(id = 180, value = "FirstResult/maxResults specified on polymorphic query; applying in memory!")
	void needsLimit();

	// Same method that exists in CoreMessageLogger
	@LogMessage(level = WARN)
	@Message(id = 245, value = "Manipulation query [%s] resulted in [%s] split queries" )
	void splitQueries(String sourceQuery, int length);

	// Same method that exists in CoreMessageLogger
	@LogMessage(level = INFO)
	@Message(id = 327, value = "Error performing load command")
	void unableToLoadCommand(@Cause HibernateException e);

	// Same method that exists in CoreMessageLogger
	@LogMessage(level = ERROR)
	@Message(id = 353, value = "Could not release a cache lock : %s" )
	void unableToReleaseCacheLock(CacheException ce);

	// Same method that exists in CoreMessageLogger
	@LogMessage(level = WARN)
	@Message(id = 447, value= "Explicit use of UPGRADE_SKIPLOCKED in lock() calls is not recommended; use normal UPGRADE locking instead")
	void explicitSkipLockedLockCombo();

	@LogMessage(level = WARN)
	@Message(id = 448, value = "Warnings creating temp table : %s")
	void warningsCreatingTempTable(SQLWarning warning);
}
