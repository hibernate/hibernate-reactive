package org.hibernate.rx.event.spi;



import java.io.Serializable;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.LoadEvent;
import org.hibernate.event.spi.LoadEventListener.LoadType;

/**
 * Defines the contract for handling of load events generated from a session.
 *
 * @author Steve Ebersole
 */
public interface RxLoadEventListener extends Serializable {

    /**
     * Handle the given load event.
     *
     * @param event The load event to be handled.
     *
     * @throws HibernateException
     */
    public CompletionStage<Void> rxOnLoad(LoadEvent event, LoadType loadType) throws HibernateException;

}
