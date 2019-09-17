package org.hibernate.rx.event.spi;


import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.DeleteEvent;

/**
 * Defines the contract for handling of deletion events generated from a session.
 *
 * @author Steve Ebersole
 */
public interface RxDeleteEventListener extends Serializable {

	/**
	 * Handle the given delete event.
	 *
	 * @param event The delete event to be handled.
	 *
	 * @throws HibernateException
	 */
	public CompletionStage<Void> rxOnDelete(DeleteEvent event) throws HibernateException;

	public CompletionStage<Void> rxOnDelete(DeleteEvent event, Set transientEntities) throws HibernateException;
}
