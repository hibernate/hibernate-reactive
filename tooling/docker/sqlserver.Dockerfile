# Microsoft SQL Server
# See https://hub.docker.com/_/microsoft-mssql-server
# NOTE: Only the FROM value is used by Testcontainers in our test suite.
# The rest of this file is for running the container manually (see podman.md).
FROM mcr.microsoft.com/mssql/server:2025-latest
ENV ACCEPT_EULA=Y
ENV MSSQL_SA_PASSWORD=~!HReact!~
EXPOSE 1433
