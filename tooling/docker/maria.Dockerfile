# MariaDB
# See https://hub.docker.com/_/mariadb
# NOTE: Only the FROM value is used by Testcontainers in our test suite.
# The rest of this file is for running the container manually (see podman.md).
FROM docker.io/mariadb:12.2.2
ENV MARIADB_USER=hreact
ENV MARIADB_PASSWORD=hreact
ENV MARIADB_DATABASE=hreact
ENV MARIADB_ROOT_PASSWORD=hreact
EXPOSE 3306
