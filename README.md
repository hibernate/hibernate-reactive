
# Hibernate RX

Experiments to see how a Reactive API could look like
when it involves non trivial database access needs.

## Testing

At this stage, only a PostgreSQL database is required.

You can quickly start one via podman:

    sudo podman run --ulimit memlock=-1:-1 -it --rm=true --memory-swappiness=0 --name HibernateTestingPGSQL -e POSTGRES_USER=hibernate-rx -e POSTGRES_PASSWORD=hibernate-rx -e POSTGRES_DB=hibernate-rx -p 5432:5432 postgres:12

