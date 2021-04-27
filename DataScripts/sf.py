#!/usr/bin/env python
from snowflake.connector.connection import SnowflakeConnection
from cfg import SnowflakeCfg
import snowflake.connector


def sf_connect(cfg: SnowflakeCfg):

    creds = cfg.creds.split(':')
    account = '.'.join(cfg.host.split('.')[:3])

    # Gets the version
    ctx = snowflake.connector.connect(
        user=creds[0],
        password=creds[1],
        account=account,
        warehouse=cfg.warehouse,
        database=cfg.db,
        schema=cfg.schema,
        role=cfg.role
    )

    return ctx


def sf_test(db: SnowflakeConnection):
    cs = db.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
