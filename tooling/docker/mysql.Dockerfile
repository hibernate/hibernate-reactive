# MySQL
# See https://hub.docker.com/_/mysql
# NOTE: Only the FROM value is used by Testcontainers in our test suite.
# The rest of this file is for running the container manually (see podman.md).
FROM container-registry.oracle.com/mysql/community-server:9.6.0
ENV MYSQL_USER=hreact
ENV MYSQL_PASSWORD=hreact
ENV MYSQL_DATABASE=hreact
ENV MYSQL_ROOT_PASSWORD=hreact
EXPOSE 3306
