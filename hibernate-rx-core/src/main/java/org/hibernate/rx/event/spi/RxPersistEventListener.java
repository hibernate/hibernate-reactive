/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.event.spi;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.PersistEvent;

/**
 * Defines the contract for handling of create events generated from a session.
 *
 * @author Gavin King
 */
public interface RxPersistEventListener extends Serializable {

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 *
	 * @throws HibernateException
	 */
	public CompletionStage<Void> rxOnPersist(PersistEvent event) throws HibernateException;

	/**
	 * Handle the given create event.
	 *
	 * @param event The create event to be handled.
	 *
	 * @throws HibernateException
	 */
	public CompletionStage<Void> rxOnPersist(PersistEvent event, Map createdAlready) throws HibernateException;

}
