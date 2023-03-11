/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.services;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.reactive.provider.impl.ReactiveServiceInitiators;
import org.hibernate.service.StandardServiceInitiators;

import org.junit.Assert;
import org.junit.Test;

/**
 * Useful to spot inconsistencies in the default ServiceInitiator lists
 * used by Hibernate Reactive compared to Hibernate ORM.
 * N.B. Hibernate Reactive defines some additional services.
 */
public class ServiceInitiatorsTest {

	private static final Map<String,String> HR_SERVICES = toServicesMap( ReactiveServiceInitiators.LIST );
	private static final Map<String,String> ORM_SERVICES = toServicesMap( StandardServiceInitiators.LIST );

	// These services are NOT provided by the Hibernate Reactive default initiators, and that should be fine:
	private static final Set<String> HR_INTENTIONALLY_OMITTED = Set.of( "org.hibernate.engine.transaction.jta.platform.spi.JtaPlatformResolver" );

	@Test
	public void serviceInitiatorsAreUnique() {
		Assert.assertEquals( HR_SERVICES.size(), ReactiveServiceInitiators.LIST.size() );
		Assert.assertEquals( ORM_SERVICES.size(), StandardServiceInitiators.LIST.size() );
	}

	@Test
	public void allORMServicesAreDefined() {
		Set<String> sharedServiceImplementations = new TreeSet<>();
		Set<String> notSharedServiceImplementations = new TreeSet<>();
		reportDivider( "All Services in sorted order" );
		int i = 1;
		for ( String key : ORM_SERVICES.keySet() ) {
			String error = "ORM service '" + key + "' is not defined by the HR services list";
			Assert.assertTrue( error, HR_SERVICES.containsKey( key ) || HR_INTENTIONALLY_OMITTED.contains( key ) );
			if ( Objects.equals( HR_SERVICES.get( key ), ORM_SERVICES.get( key ) ) ) {
				sharedServiceImplementations.add( key );
			}
			else {
				notSharedServiceImplementations.add( key );
			}
			StringBuilder entry = new StringBuilder();
			entry.append( " #" ).append( i++ ).append( '\t' ).append( "service role " ).append( key ).append( '\n' );
			entry.append( "\tORM: " ).append( ORM_SERVICES.get( key ) ).append( '\n' );
			entry.append( "\tHR:  " ).append( HR_SERVICES.get( key ) ).append( '\n' );
			System.out.println( entry );
		}
		reportDivider( "All Services which are shared among ORM and HR:" );
		for ( String key : sharedServiceImplementations ) {
			System.out.println( key );
		}
		reportDivider( "All Services which are overridden in HR:" );
		for ( String key : notSharedServiceImplementations ) {
			StringBuilder entry = new StringBuilder();
			entry.append( " service role " ).append( key ).append( '\n' );
			entry.append( "\tORM: " ).append( ORM_SERVICES.get( key ) ).append( '\n' );
			entry.append( "\tHR:  " ).append( HR_SERVICES.get( key ) ).append( '\n' );
			System.out.println( entry );
		}
	}

	private void reportDivider(String title) {
		System.out.println( "\n ########### " + title + "\n" );
	}

	//N.B. service identity and class identity are handled by their name:
	//this is possibly more restrictive than strictly required in certain environments,
	//but we need to ensure safety in all classloading models.
	private static Map<String, String> toServicesMap(List<StandardServiceInitiator<?>> list) {
		TreeMap<String,String> rolesToImplMap = new TreeMap<>();
		for ( StandardServiceInitiator<?> initiator : list ) {
			final String serviceRole = initiator.getServiceInitiated().getName();
			rolesToImplMap.put( serviceRole, initiator.getClass().getName() );
		}
		return Collections.unmodifiableMap( rolesToImplMap );
	}

}
