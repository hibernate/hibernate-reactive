package org.hibernate.rx.service.initiator;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.service.Service;

public interface RxConnectionPoolProvider extends Service {
	RxConnection getConnection();
}
