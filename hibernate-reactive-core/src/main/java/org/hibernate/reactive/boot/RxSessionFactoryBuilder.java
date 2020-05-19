/*
 * Hibernate Reactive
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.boot;

import org.hibernate.*;
import org.hibernate.boot.SessionFactoryBuilder;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.loader.BatchFetchStyle;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.reactive.stage.RxSessionFactory;

import java.util.Map;

/**
 * A {@link SessionFactoryBuilder builder} for the {@link RxSessionFactory}.
 *
 * @see RxSessionFactory
 * @see org.hibernate.reactive.boot.impl.RxSessionFactoryBuilderImpl
 */
public interface RxSessionFactoryBuilder extends SessionFactoryBuilder {

	@Override
	RxSessionFactoryBuilder applyValidatorFactory(Object validatorFactory);

	@Override
	RxSessionFactoryBuilder applyBeanManager(Object beanManager);

	@Override
	RxSessionFactoryBuilder applyName(String sessionFactoryName);

	@Override
	RxSessionFactoryBuilder applyNameAsJndiName(boolean isJndiName);

	@Override
	RxSessionFactoryBuilder applyAutoClosing(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyAutoFlushing(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyStatisticsSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyInterceptor(Interceptor interceptor);

	@Override
	RxSessionFactoryBuilder addSessionFactoryObservers(SessionFactoryObserver... observers);

	@Override
	RxSessionFactoryBuilder applyCustomEntityDirtinessStrategy(CustomEntityDirtinessStrategy strategy);

	@Override
	RxSessionFactoryBuilder addEntityNameResolver(EntityNameResolver... entityNameResolvers);

	@Override
	RxSessionFactoryBuilder applyEntityNotFoundDelegate(EntityNotFoundDelegate entityNotFoundDelegate);

	@Override
	RxSessionFactoryBuilder applyIdentifierRollbackSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyNullabilityChecking(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyLazyInitializationOutsideTransaction(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyBatchFetchStyle(BatchFetchStyle style);

	@Override
	RxSessionFactoryBuilder applyDefaultBatchFetchSize(int size);

	@Override
	RxSessionFactoryBuilder applyMaximumFetchDepth(int depth);

	@Override
	RxSessionFactoryBuilder applyDefaultNullPrecedence(NullPrecedence nullPrecedence);

	@Override
	RxSessionFactoryBuilder applyOrderingOfInserts(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyOrderingOfUpdates(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyMultiTenancyStrategy(MultiTenancyStrategy strategy);

	@Override
	RxSessionFactoryBuilder applyCurrentTenantIdentifierResolver(CurrentTenantIdentifierResolver resolver);

	@Override
	@Deprecated
	RxSessionFactoryBuilder applyJtaTrackingByThread(boolean enabled);

	@Override
	@Deprecated
	RxSessionFactoryBuilder applyQuerySubstitutions(Map substitutions);

	@Override
	RxSessionFactoryBuilder applyStrictJpaQueryLanguageCompliance(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyNamedQueryCheckingOnStartup(boolean enabled);

	@Override
	RxSessionFactoryBuilder applySecondLevelCacheSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyQueryCacheSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyCacheRegionPrefix(String prefix);

	@Override
	RxSessionFactoryBuilder applyMinimalPutsForCaching(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyStructuredCacheEntries(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyDirectReferenceCaching(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyAutomaticEvictionOfCollectionCaches(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyJdbcBatchSize(int size);

	@Override
	RxSessionFactoryBuilder applyJdbcBatchingForVersionedEntities(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyScrollableResultsSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyResultSetsWrapping(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyGetGeneratedKeysSupport(boolean enabled);

	@Override
	RxSessionFactoryBuilder applyJdbcFetchSize(int size);

	@Override
	RxSessionFactoryBuilder applyConnectionReleaseMode(ConnectionReleaseMode connectionReleaseMode);

	@Override
	RxSessionFactoryBuilder applySqlComments(boolean enabled);

	@Override
	RxSessionFactory build();
}
