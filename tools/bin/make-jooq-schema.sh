#!/bin/bash
# make-jooq-schema.sh — Generates jOOQ classes from current airbyte/ DB migrations
# Bypasses Testcontainers entirely by:
#   1. Starting a real PostgreSQL container
#   2. Applying initial schema + Flyway migrations
#   3. Running jOOQ codegen with standard PostgresDatabase
#
# Usage: ./tools/bin/make-jooq-schema.sh
# After running: ./build.sh distTar

set -e

# Ensure we are running from the project root
cd "$(dirname "$0")/../.."

JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@21}"
PG_CONTAINER_NAME="airbyte-jooq-gen-pg"
PG_PORT=15432
PG_USER="jooq_generator"
PG_PASSWORD="jooq_generator"
CONFIGS_DB="jooq_airbyte_configs"
JOBS_DB="jooq_airbyte_jobs"

OUTPUT_DIR_CONFIGS="airbyte-db/jooq/build/generated/configsDatabase/src/main/java"
OUTPUT_DIR_JOBS="airbyte-db/jooq/build/generated/jobsDatabase/src/main/java"

RUNNER_DIR="/tmp/jooq-gen-runner"

cleanup() {
  echo "Cleaning up..."
  docker rm -f "$PG_CONTAINER_NAME" 2>/dev/null || true
  rm -rf "$RUNNER_DIR" 2>/dev/null || true
}
trap cleanup EXIT

# ─── Step 1: Start PostgreSQL ───────────────────────────────────────
echo "Starting PostgreSQL..."
docker rm -f "$PG_CONTAINER_NAME" 2>/dev/null || true
docker run -d --name "$PG_CONTAINER_NAME" \
  -e POSTGRES_USER="$PG_USER" \
  -e POSTGRES_PASSWORD="$PG_PASSWORD" \
  -p "$PG_PORT:5432" \
  postgres:13-alpine >/dev/null

for i in $(seq 1 30); do
  docker exec "$PG_CONTAINER_NAME" pg_isready -U "$PG_USER" >/dev/null 2>&1 && break
  sleep 1
done

docker exec "$PG_CONTAINER_NAME" psql -U "$PG_USER" -c "CREATE DATABASE $CONFIGS_DB;" 2>/dev/null || true
docker exec "$PG_CONTAINER_NAME" psql -U "$PG_USER" -c "CREATE DATABASE $JOBS_DB;" 2>/dev/null || true

# Apply initial schemas (required before Flyway migrations)
echo "Applying initial schemas..."
docker exec -i "$PG_CONTAINER_NAME" psql -U "$PG_USER" -d "$CONFIGS_DB" < airbyte-db/db-lib/src/main/resources/configs_database/schema.sql
docker exec -i "$PG_CONTAINER_NAME" psql -U "$PG_USER" -d "$JOBS_DB" < airbyte-db/db-lib/src/main/resources/jobs_database/schema.sql
echo "PostgreSQL ready on port $PG_PORT"

# ─── Step 2: Compile dependencies ──────────────────────────────────
echo "Compiling database libraries..."
JAVA_HOME="$JAVA_HOME" ./gradlew \
  :oss:airbyte-db:db-lib:jar \
  :oss:airbyte-commons:jar \
  :oss:airbyte-config:config-models:jar \
  --no-daemon -x test -x spotlessCheck -x spotlessApply \
  -x :oss:airbyte-db:jooq:generateConfigsDatabaseJooq \
  -x :oss:airbyte-db:jooq:generateJobsDatabaseJooq \
  -q 2>&1
echo "Dependencies compiled."

# ─── Step 3: Build classpath ───────────────────────────────────────
echo "Building classpath for generator..."
mkdir -p "$RUNNER_DIR"
cat > "$RUNNER_DIR/print-cp.gradle" <<'EOF'
allprojects {
  afterEvaluate {
    tasks.register("printJooqGeneratorClasspath") {
      doLast {
        def conf = configurations.findByName("jooqGenerator")
        if (conf) {
          println conf.files.join(":")
        }
      }
    }
  }
}
EOF

CLASSPATH=$(JAVA_HOME="$JAVA_HOME" ./gradlew --init-script "$RUNNER_DIR/print-cp.gradle" -q :oss:airbyte-db:jooq:printJooqGeneratorClasspath --no-daemon 2>/dev/null | tail -n 1)

echo "Classpath built with $(echo "$CLASSPATH" | tr ':' '\n' | wc -l | tr -d ' ') JARs."

# ─── Step 4: Create and run migration runner ──────────────────────
mkdir -p "$RUNNER_DIR"
cat > "$RUNNER_DIR/MigrationRunner.java" <<'JAVA_EOF'
import org.flywaydb.core.Flyway;
import org.flywaydb.database.postgresql.PostgreSQLConfigurationExtension;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MigrationRunner {
    public static void main(String[] args) throws Exception {
        String jdbcUrl = args[0];
        String user = args[1];
        String password = args[2];
        String dbIdentifier = args[3];
        String[] locations = new String[args.length - 4];
        System.arraycopy(args, 4, locations, 0, locations.length);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(2);

        try (HikariDataSource ds = new HikariDataSource(config)) {
            org.flywaydb.core.api.configuration.FluentConfiguration flywayConfig = Flyway.configure()
                .dataSource(ds)
                .baselineVersion("0.29.0.001")
                .baselineDescription("Baseline from file-based migration v1")
                .baselineOnMigrate(true)
                .installedBy("jooq-generator")
                .table("airbyte_" + dbIdentifier + "_migrations")
                .locations(locations);
                
            flywayConfig.getPluginRegister()
                .getPlugin(PostgreSQLConfigurationExtension.class)
                .setTransactionalLock(false);

            Flyway flyway = flywayConfig.load();
            var result = flyway.migrate();
            System.out.println("Applied " + result.migrationsExecuted + " migrations for " + dbIdentifier);
        }
    }
}
JAVA_EOF

echo "Compiling migration runner..."
"$JAVA_HOME/bin/javac" -cp "$CLASSPATH" "$RUNNER_DIR/MigrationRunner.java" -d "$RUNNER_DIR" 2>&1

echo "Running configurations migrations..."
"$JAVA_HOME/bin/java" -cp "$CLASSPATH:$RUNNER_DIR" MigrationRunner \
  "jdbc:postgresql://localhost:$PG_PORT/$CONFIGS_DB" "$PG_USER" "$PG_PASSWORD" "configs" \
  "classpath:io/airbyte/db/instance/configs/migrations" 2>&1

echo "Running jobs migrations..."
"$JAVA_HOME/bin/java" -cp "$CLASSPATH:$RUNNER_DIR" MigrationRunner \
  "jdbc:postgresql://localhost:$PG_PORT/$JOBS_DB" "$PG_USER" "$PG_PASSWORD" "jobs" \
  "classpath:io/airbyte/db/instance/jobs/migrations" 2>&1


# ─── Step 5: Run jOOQ codegen ──────────────────────────────────────
mkdir -p "$OUTPUT_DIR_CONFIGS" "$OUTPUT_DIR_JOBS"

# Use standard PostgresDatabase — our DB already has the full schema from migrations  
CONFIGS_XML=$(mktemp -t jooq-configs)
cat > "$CONFIGS_XML" <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<configuration xmlns="http://www.jooq.org/xsd/jooq-codegen-3.19.0.xsd">
  <jdbc>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:$PG_PORT/$CONFIGS_DB</url>
    <user>$PG_USER</user>
    <password>$PG_PASSWORD</password>
  </jdbc>
  <generator>
    <name>org.jooq.codegen.DefaultGenerator</name>
    <database>
      <name>org.jooq.meta.postgres.PostgresDatabase</name>
      <inputSchema>public</inputSchema>
      <excludes>airbyte_configs_migrations|flyway_schema_history</excludes>
    </database>
    <target>
      <packageName>io.airbyte.db.instance.configs.jooq.generated</packageName>
      <directory>$(pwd)/$OUTPUT_DIR_CONFIGS</directory>
    </target>
  </generator>
</configuration>
EOF

JOBS_XML=$(mktemp -t jooq-jobs)
cat > "$JOBS_XML" <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<configuration xmlns="http://www.jooq.org/xsd/jooq-codegen-3.19.0.xsd">
  <jdbc>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:$PG_PORT/$JOBS_DB</url>
    <user>$PG_USER</user>
    <password>$PG_PASSWORD</password>
  </jdbc>
  <generator>
    <name>org.jooq.codegen.DefaultGenerator</name>
    <database>
      <name>org.jooq.meta.postgres.PostgresDatabase</name>
      <inputSchema>public</inputSchema>
      <excludes>airbyte_jobs_migrations|flyway_schema_history</excludes>
    </database>
    <target>
      <packageName>io.airbyte.db.instance.jobs.jooq.generated</packageName>
      <directory>$(pwd)/$OUTPUT_DIR_JOBS</directory>
    </target>
  </generator>
</configuration>
EOF

echo "Generating jOOQ classes for configurations database..."
"$JAVA_HOME/bin/java" -cp "$CLASSPATH" org.jooq.codegen.GenerationTool "$CONFIGS_XML" 2>&1

echo "Generating jOOQ classes for jobs database..."
"$JAVA_HOME/bin/java" -cp "$CLASSPATH" org.jooq.codegen.GenerationTool "$JOBS_XML" 2>&1

rm -f "$CONFIGS_XML" "$JOBS_XML"

# ─── Step 6: Verify ───────────────────────────────────────────────
echo ""
echo "Verifying generated classes..."
if grep -q "ON_DEMAND_ENABLED" "$OUTPUT_DIR_CONFIGS/io/airbyte/db/instance/configs/jooq/generated/tables/Connection.java" 2>/dev/null; then
  echo "Verification success: ON_DEMAND_ENABLED found."
else
  echo "Verification warning: ON_DEMAND_ENABLED not found."
fi

CONFIGS_COUNT=$(find "$OUTPUT_DIR_CONFIGS" -name "*.java" 2>/dev/null | wc -l | tr -d ' ')
JOBS_COUNT=$(find "$OUTPUT_DIR_JOBS" -name "*.java" 2>/dev/null | wc -l | tr -d ' ')
echo "Generated $CONFIGS_COUNT configuration classes and $JOBS_COUNT jobs classes."
echo ""
echo "Execution completed. You can now run: ./build.sh distTar"
