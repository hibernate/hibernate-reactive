package org.hibernate.rx;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class TestConfiguration {
  
  private static final boolean USE_DOCKER = Boolean.getBoolean("docker");
  
  private static final MySQLContainer<?> mysql = new MySQLContainer<>()
        .withUsername("hibernate-rx")
        .withPassword("hibernate-rx")
        .withDatabaseName("hibernate-rx")
        .withReuse(true);
  
  private static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>()
        .withUsername("hibernate-rx")
        .withPassword("hibernate-rx")
        .withDatabaseName("hibernate-rx")
        .withReuse(true);
  
  public static String getPostgreSQLURL() {
    if (USE_DOCKER) {
      postgresql.start();
      return postgresql.getJdbcUrl() +  "&user=" + postgresql.getUsername() + "&password=" + postgresql.getPassword();
    } else {
      return "jdbc:postgresql://localhost:5432/hibernate-rx?user=hibernate-rx&password=hibernate-rx";
    }
  }
  
  public static String getMySQLURL() {
    if (USE_DOCKER) {
      mysql.start();
      return mysql.getJdbcUrl() +  "?user=" + mysql.getUsername() + "&password=" + mysql.getPassword();
    } else {
      return "jdbc:mysql://localhost:3306/hibernate-rx?user=hibernate-rx&password=hibernate-rx";
    }
  }

}
