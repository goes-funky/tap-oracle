import singer
import cx_Oracle

LOGGER = singer.get_logger()


def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(schema, table, column)


def make_dsn(config, sid=False):
    if sid:
        return cx_Oracle.makedsn(config["host"], config["port"], config["sid"])
    return cx_Oracle.makedsn(config["host"], config["port"], service_name=config["sid"])


def connect(config):
    try:
        connection = cx_Oracle.connect(config["user"], config["password"], make_dsn(config, sid=True))
        return connection
    except Exception as e:
        try:
            connection = cx_Oracle.connect(config["user"], config["password"], make_dsn(config))
            return connection
        except Exception as e:
            raise Exception("Connection could not be achieved! {}".format(e))


def open_connection(config):
    LOGGER.info("dsn: %s", make_dsn(config))
    # INFO dsn: (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=ORCLCDB)))
    try:
        conn = connect(config)
        return conn
    except Exception as e:
        raise Exception("Connection could not be achieved! {}".format(e))
