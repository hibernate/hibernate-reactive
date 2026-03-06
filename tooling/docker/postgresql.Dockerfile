# PostgreSQL
# See https://hub.docker.com/_/postgres
# NOTE: Only the FROM value is used by Testcontainers in our test suite.
# The rest of this file is for running the container manually (see podman.md).
FROM docker.io/postgres:18.3
ENV POSTGRES_USER=hreact
ENV POSTGRES_PASSWORD=hreact
ENV POSTGRES_DB=hreact
EXPOSE 5432
