/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.logging.impl;



import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.FormatWith;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

@MessageLogger(projectCode = "HR")
public interface Log extends BasicLogger {

	@LogMessage(level = INFO)
	@Message(id = 1, value = "Hibernate Reactive Preview")
	void startHibernateReactive();

	@LogMessage(level = INFO)
	@Message(id = 11, value = "SQL Client URL [%1$s]")
	void sqlClientUrl(String url);

	@LogMessage(level = INFO)
	@Message(id = 13, value = "Detected driver [%1$s]")
	void detectedDriver(String driverName);

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
}
