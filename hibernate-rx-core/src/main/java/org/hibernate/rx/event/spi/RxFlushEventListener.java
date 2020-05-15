/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.rx.event.spi;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.FlushEvent;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

/**
 * Defines the contract for handling of session flush events.
 *default
 * @author Steve Ebersole
 */
public interface RxFlushEventListener extends Serializable {
	/**
	 * Handle the given flush event.
	 *
	 * @param event The flush event to be handled.
	 */
	CompletionStage<Void> rxOnFlush(FlushEvent event) throws HibernateException;
}
