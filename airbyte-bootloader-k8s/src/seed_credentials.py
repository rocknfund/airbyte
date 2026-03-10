"""
Background credential seeder for Airbyte cold start.

Waits for bootloader Flyway migrations to create the dataplane,
then inserts a service account matching the fixed DATAPLANE_CLIENT_ID/SECRET
from environment variables into PostgreSQL.

This ensures that server/launcher/cron can authenticate with a deterministic
client ID after a cold start (docker compose down -v && up).
"""

import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(name)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('seed')

MAX_ATTEMPTS = 60
RETRY_INTERVAL = 5


def seed():
    client_id = os.environ.get('DATAPLANE_CLIENT_ID')
    client_secret = os.environ.get('DATAPLANE_CLIENT_SECRET')

    if not client_id or not client_secret:
        log.warning('DATAPLANE_CLIENT_ID or DATAPLANE_CLIENT_SECRET not set, skipping.')
        return

    db_config = {
        'host': os.environ.get('DATABASE_HOST', 'db'),
        'port': int(os.environ.get('DATABASE_PORT', '5432')),
        'database': os.environ.get('DATABASE_DB', 'db-airbyte'),
        'user': os.environ.get('DATABASE_USER', 'docker'),
        'password': os.environ.get('DATABASE_PASSWORD', 'docker'),
    }

    import pg8000

    for attempt in range(1, MAX_ATTEMPTS + 1):
        conn = None
        try:
            conn = pg8000.connect(**db_config)
            conn.autocommit = False
            cur = conn.cursor()

            # Skip if already seeded
            cur.execute("SELECT 1 FROM service_accounts WHERE id = %s", (client_id,))
            if cur.fetchone():
                log.info('Already seeded, skipping.')
                return

            # Wait for bootloader to create dataplane
            cur.execute("SELECT id FROM dataplane LIMIT 1")
            dp_row = cur.fetchone()
            if not dp_row:
                raise RuntimeError('dataplane not yet created')
            dataplane_id = dp_row[0]

            # 1. Insert service account with fixed ID
            cur.execute(
                """INSERT INTO service_accounts (id, name, secret, managed, created_at, updated_at)
                   VALUES (%s, 'dataplane-docker', %s, true, now(), now())""",
                (client_id, client_secret),
            )

            # 2. Create permission (required by server auth chain)
            cur.execute(
                """INSERT INTO permission (id, service_account_id, permission_type, created_at, updated_at)
                   VALUES (gen_random_uuid(), %s, 'dataplane', now(), now())""",
                (client_id,),
            )

            # 3. Create client credentials linking to dataplane
            cur.execute(
                """INSERT INTO dataplane_client_credentials (id, dataplane_id, client_id, client_secret, created_at)
                   VALUES (gen_random_uuid(), %s, %s, %s, now())
                   ON CONFLICT (client_id) DO NOTHING""",
                (dataplane_id, client_id, client_secret),
            )

            # 4. Link service account to dataplane
            cur.execute(
                "UPDATE dataplane SET service_account_id = %s WHERE id = %s",
                (client_id, dataplane_id),
            )

            conn.commit()
            log.info('Service account %s linked to dataplane %s.', client_id, dataplane_id)
            return

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            log.info('Attempt %d/%d: %s', attempt, MAX_ATTEMPTS, e)
            time.sleep(RETRY_INTERVAL)

        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    log.error('Failed to seed credentials after %d attempts.', MAX_ATTEMPTS)
    sys.exit(1)


if __name__ == '__main__':
    seed()
