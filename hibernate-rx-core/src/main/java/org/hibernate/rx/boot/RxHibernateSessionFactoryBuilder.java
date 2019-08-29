/*
 * Hibernate Reactive
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.boot;

import java.util.Map;

import org.hibernate.ConnectionReleaseMode;
import org.hibernate.CustomEntityDirtinessStrategy;
import org.hibernate.EntityNameResolver;
import org.hibernate.Interceptor;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.NullPrecedence;
import org.hibernate.SessionFactoryObserver;
import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.loader.BatchFetchStyle;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.rx.RxHibernateSessionFactory;

/**
 * A {@link SessionFactoryBuilder} for the {@link RxHibernateSessionFactory}
 */
public interface RxHibernateSessionFactoryBuilder extends SessionFactoryBuilder {

	@Override
	RxHibernateSessionFactoryBuilder applyValidatorFactory(Object validatorFactory);

	@Override
	RxHibernateSessionFactoryBuilder applyBeanManager(Object beanManager);

	@Override
	RxHibernateSessionFactoryBuilder applyName(String sessionFactoryName);

	@Override
	RxHibernateSessionFactoryBuilder applyNameAsJndiName(boolean isJndiName);

	@Override
	RxHibernateSessionFactoryBuilder applyAutoClosing(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyAutoFlushing(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyStatisticsSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyInterceptor(Interceptor interceptor);

	@Override
	RxHibernateSessionFactoryBuilder addSessionFactoryObservers(SessionFactoryObserver... observers);

	@Override
	RxHibernateSessionFactoryBuilder applyCustomEntityDirtinessStrategy(CustomEntityDirtinessStrategy strategy);

	@Override
	RxHibernateSessionFactoryBuilder addEntityNameResolver(EntityNameResolver... entityNameResolvers);

	@Override
	RxHibernateSessionFactoryBuilder applyEntityNotFoundDelegate(EntityNotFoundDelegate entityNotFoundDelegate);

	@Override
	RxHibernateSessionFactoryBuilder applyIdentifierRollbackSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyNullabilityChecking(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyLazyInitializationOutsideTransaction(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyBatchFetchStyle(BatchFetchStyle style);

	@Override
	RxHibernateSessionFactoryBuilder applyDefaultBatchFetchSize(int size);

	@Override
	RxHibernateSessionFactoryBuilder applyMaximumFetchDepth(int depth);

	@Override
	RxHibernateSessionFactoryBuilder applyDefaultNullPrecedence(NullPrecedence nullPrecedence);

	@Override
	RxHibernateSessionFactoryBuilder applyOrderingOfInserts(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyOrderingOfUpdates(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyMultiTenancyStrategy(MultiTenancyStrategy strategy);

	@Override
	RxHibernateSessionFactoryBuilder applyCurrentTenantIdentifierResolver(CurrentTenantIdentifierResolver resolver);

	@Override
	@Deprecated
	RxHibernateSessionFactoryBuilder applyJtaTrackingByThread(boolean enabled);

	@Override
	@Deprecated
	RxHibernateSessionFactoryBuilder applyQuerySubstitutions(Map substitutions);

	@Override
	RxHibernateSessionFactoryBuilder applyStrictJpaQueryLanguageCompliance(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyNamedQueryCheckingOnStartup(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applySecondLevelCacheSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyQueryCacheSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyCacheRegionPrefix(String prefix);

	@Override
	RxHibernateSessionFactoryBuilder applyMinimalPutsForCaching(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyStructuredCacheEntries(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyDirectReferenceCaching(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyAutomaticEvictionOfCollectionCaches(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyJdbcBatchSize(int size);

	@Override
	RxHibernateSessionFactoryBuilder applyJdbcBatchingForVersionedEntities(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyScrollableResultsSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyResultSetsWrapping(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyGetGeneratedKeysSupport(boolean enabled);

	@Override
	RxHibernateSessionFactoryBuilder applyJdbcFetchSize(int size);

	@Override
	RxHibernateSessionFactoryBuilder applyConnectionReleaseMode(ConnectionReleaseMode connectionReleaseMode);

	@Override
	RxHibernateSessionFactoryBuilder applySqlComments(boolean enabled);

	@Override
	RxHibernateSessionFactory build();
}
