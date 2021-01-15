/*
 * MIT License
 *
 * Copyright (c) 2020 Airbyte
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package io.airbyte.server.converters;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.resources.MoreResources;
import io.airbyte.db.Database;
import io.airbyte.db.Databases;
import io.airbyte.scheduler.persistence.DefaultJobPersistence;
import io.airbyte.scheduler.persistence.JobPersistence;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

public class DatabaseArchiverTest {

  private PostgreSQLContainer<?> container;
  private Database database;
  private DatabaseArchiver databaseArchiver;

  @BeforeEach
  void setUp() throws IOException, SQLException {
    container = new PostgreSQLContainer<>("postgres:13-alpine");
    container.start();
    final JsonNode dbConfig = Jsons.jsonNode(ImmutableMap.builder()
        .put("username", container.getUsername())
        .put("password", container.getPassword())
        .put("jdbc_url", String.format("jdbc:postgresql://%s:%s/%s",
            container.getHost(),
            container.getFirstMappedPort(),
            container.getDatabaseName()))
        .build());
    database = Databases.createPostgresDatabase(
        dbConfig.get("username").asText(),
        dbConfig.get("password").asText(),
        dbConfig.get("jdbc_url").asText());
    JobPersistence persistence = new DefaultJobPersistence(database);
    final String schemaFile = MoreResources.readResource("schema.sql");
    final String sql = schemaFile.substring(schemaFile.indexOf("-- Statements Below"), schemaFile.indexOf("-- Statements Above"));
    database.query(ctx -> ctx.execute(sql));
    databaseArchiver = new DatabaseArchiver(persistence);
  }

  @AfterEach
  void tearDown() throws Exception {
    database.close();
    container.close();
  }

  @Test
  void testWrongDatabaseMigration() throws SQLException, IOException {
    database.query(ctx -> {
      ctx.fetch("CREATE TABLE id_and_name(id INTEGER, name VARCHAR(200), updated_at DATE);");
      ctx.fetch(
          "INSERT INTO id_and_name (id, name, updated_at) VALUES (1,'picard', '2004-10-19'),  (2, 'crusher', '2005-10-19'), (3, 'vash', '2006-10-19');");
      return null;
    });
    final Path tempFolder = Files.createTempDirectory("testWrongDatabaseMigration");
    assertThrows(RuntimeException.class, () -> databaseArchiver.exportDatabaseToArchive(tempFolder));
  }

  @Test
  void testDatabaseMigration() throws SQLException, IOException {
    database.query(ctx -> {
      ctx.fetch(
          "INSERT INTO jobs(id, scope, config_type, config, status, created_at, started_at, updated_at) VALUES "
              + "(1,'get_spec_scope', 'get_spec', '{ \"type\" : \"getSpec\" }', 'succeeded', '2004-10-19', null, '2004-10-19'), "
              + "(2,'sync_scope', 'sync', '{ \"job\" : \"sync\" }', 'running', '2005-10-19', null, '2005-10-19'), "
              + "(3,'sync_scope', 'sync', '{ \"job\" : \"sync\" }', 'pending', '2006-10-19', null, '2006-10-19');");
      return null;
    });
    final Path tempFolder = Files.createTempDirectory("testWrongDatabaseMigration");
    databaseArchiver.exportDatabaseToArchive(tempFolder);
    databaseArchiver.importDatabaseFromArchive(tempFolder);
    // TODO check database state before/after
  }

}
