#!/usr/bin/env bash
set -euo pipefail

: "${DB_PASSWORD:?DB_PASSWORD must be set}"

echo "Waiting for PostgreSQL..."
until pg_isready -h localhost -U system -d wise -q 2>/dev/null; do
    sleep 1
done
echo "PostgreSQL is ready."

# Check whether the database has already been initialised (idempotent).
export PGPASSWORD="$DB_PASSWORD"
if psql -h localhost -U system -d wise -c "SELECT 1 FROM aweprojects LIMIT 1" \
        >/dev/null 2>&1; then
    echo "Database already initialised — skipping setup."
    exit 0
fi

echo "Initialising database schema..."
python -u -c "
import os, sys

# Ensure AWETARGET is set so commonwise reads the right Environment.cfg.
os.environ.setdefault('AWETARGET', 'metiswise')

# On some systems zlib must be imported before psycopg2.
import zlib  # noqa: F401
import psycopg2

from common.config.Profile import profiles
from common.toolbox.backends.postgresql import dbawoper

db_pw = os.environ['DB_PASSWORD']

# Configure the database as superuser SYSTEM.
profile = profiles.create_profile(username='system', password=db_pw)
from common.database.Database import database

try:
    dbawoper.configure_database(1, '-9', profile.password)
except psycopg2.errors.UndefinedObject:
    dbawoper.configure_database(1, -9, profile.password)

from common.toolbox.backends.postgresql import dbnewuser
dbnewuser.add_user('AWTEST', 'lmno')

# Reconnect as AWOPER and create persistent classes.
database.disconnect()
profiles.remove_profile()
profiles.create_profile(username='AWOPER', password=db_pw)
database.connect()

# Import MetisWISE classes to register them for table creation.
import metiswise.main.aweimports  # noqa: F401

from common.toolbox.backends.postgresql import dbflatremake
dbflatremake.create_and_execute_statements()

from common.database import Security
Security.add_project('SIM', description='project with simulations', default_privilages=2)
database.execute_insert(
    'insert into aweprojectusers (projectid, userid, usertype) '
    'select id, user_id, 1 from aweprojects, aweusers'
)

from common.toolbox.backends.postgresql import dbgrants
dbgrants.grant()

print('Database setup complete.')
"
