package org.hibernate.rx.service.initiator;

import org.hibernate.rx.service.RxConnection;
import org.hibernate.service.Service;

public interface RxConnectionPoolProvider extends Service {
	RxConnection getConnection();

	void close();
}
