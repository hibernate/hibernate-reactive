/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.reactive.event.spi;

import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.AutoFlushEvent;

public interface RxAutoFlushEventListener extends Serializable {

	CompletionStage<Void> rxOnAutoFlush(AutoFlushEvent event) throws HibernateException;

}
