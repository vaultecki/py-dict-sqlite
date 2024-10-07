import copy
import datetime
import os
import sqlite3 as sqlite
import logging


class VaultDBDict:
    def __init__(self, filename, db_layout, password="none"):
        """Try to smooth out handling sqlite as dict with get and set methods
            :param filename: name of the sql file
            :type filename: str
            :param db_layout: db layout to be created
            :type db_layout: dict
            :param password: password for database protection, optional, not used for now
            :type password: str
        """
        self.__password = password
        self.__logger = logging.getLogger(__name__)
        # check if db file exist
        db_exists = os.path.exists(str(filename))
        self.__db_layout = db_layout
        self.__filename = str(filename)
        self.__keys = {}
        self.__primary = {}
        self.__tables = []
        self.__data = {}
        self.__analyse_db_from_json()
        self.__logger.info("try to open db file {}".format(self.__filename))
        self.__db_conn = sqlite.connect(self.__filename, check_same_thread=False)
        self.__curser = self.__db_conn.cursor()
        if not db_exists:
            self.__logger.info("db file {} does not exist - creating".format(filename))
            self.__create_tables()
        # self.get_data()

    def __create_db_string_from_json(self, table, json_str):
        """Create table using given parameter
            :param table: name of the db table
            :type table: str
            :param json_str: structure of table
            :type json_str: dict
            :return: SQL command
            :rtype: str
        """
        self.__logger.info("create table {}".format(table))
        db_str = "CREATE TABLE {} (".format(table)
        for key in json_str:
            attribute = json_str.get(key)
            self.__logger.debug("table: {}; key: {}; value: {}".format(table, key, attribute))
            if type(attribute) is dict:
                for attribute in json_str.get(key, {}):
                    table_str = json_str.get(key, {})
                    db_str += self.__create_table_column(attribute, table_str.get(attribute), table)
            else:
                db_str += self.__create_table_column(key, attribute, table)
        db_str = db_str[:-1] + self.__create_table_primary(table) + ")"
        return db_str

    def __create_table_primary(self, table):
        """Create string for a multi-column primary key if needed
            :param table: name of the db table
            :type table: str
            :return: SQL statement part for a multi-column primary key
            :rtype: str
        """
        self.__logger.debug("generate primary entry for table {} creation".format(table))
        primary = self.__primary.get(table, False)
        return_string = ""
        if type(primary) is list:
            return_string = ", PRIMARY KEY ("
            for element in primary:
                return_string += "{}, ".format(element)
            return_string = return_string[:-2] + ")"
        return return_string

    def __create_table_column(self, column, attribute, table):
        """Create string for db creation for column, remove primary from attribute if multi-column primary key is needed
            :param table: name of the db table
            :type table: str
            :param column: structure of the columns for the table
            :type column: dict
            :param attribute: attributes of teh actual column
            :type attribute: str
            :return: SQL command part
            :rtype: str
        """
        primary = self.__primary.get(table, False)
        attribute = attribute.upper()
        if type(primary) is list:
            if "PRIMARY KEY" in attribute:
                attribute = attribute.replace(" PRIMARY KEY", "")
                if "NOT NULL" not in attribute:
                    attribute += " NOT NULL"
        return_string = " {} {},".format(column, attribute)
        return return_string

    def __analyse_db_from_json(self):
        """Read the given parameters, search for primary key"""
        self.__logger.debug("analyse db layout from json")
        for table in self.__db_layout:
            self.__tables.append(table)
            keys = []
            for key, value in self.__db_layout.get(table).items():
                keys.append(key)
                self.__logger.debug("table: {}; key: {}; value: {}".format(table, key, value))
                if "PRIMARY KEY" in value:
                    primary = self.__primary.get(table, False)
                    if not primary:
                        primary = key
                    else:
                        if type(primary) is not list:
                            primary = [primary]
                        primary.append(key)
                    self.__primary.update({table: primary})
            self.__logger.debug("primary key(s) for table {} are {}".format(table, self.__primary.get(table, False)))
            self.__keys.update({table: keys})

    def __create_tables(self):
        """Create tables for all layouts"""
        for table in self.__db_layout:
            create_str = self.__create_db_string_from_json(table, self.__db_layout.get(table, ""))
            self.__logger.debug("create string for db: {}".format(create_str))
            self.__use_db(create_str)

    def __disassemble_return_value(self, rdata, table):
        """Convert data from db into dict and return
            :param rdata: table data from db
            :type rdata: list
            :param table: table name
            :type table: str
            :return: data from db
            :rtype: dict
        """
        data = {}
        if len(rdata) > 0:
            primary = self.__primary.get(table, "")
            if not primary:
                return data
            for new_data in rdata:
                start_data = 1
                new_key = new_data[0]
                if type(primary) is list:
                    start_data = len(primary)
                    new_key = []
                    for i in range(start_data):
                        new_key.append(new_data[i])
                    new_key = tuple(new_key)
                new_dict = {}
                for key in self.__keys.get(table):
                    if key not in primary:
                        new_dict.update({key: new_data[start_data]})
                        start_data += 1
                data.update({new_key: new_dict})
        return data

    def get_data(self, table_name=False):
        """Get data from table
            :param table_name: name to be searched
            :type table_name: str
            :return: got tables
            :rtype: dict
        """
        self.__logger.debug("getdata for table(s)")
        for table in self.__db_layout:
            cmd = "SELECT "
            primary = self.__primary.get(table, False)
            if type(primary) is list:
                for element in primary:
                    cmd += "{}, ".format(element)
            else:
                cmd += "{}, ".format(primary)
            for key in self.__keys.get(table):
                if key not in primary:
                    cmd += "{}, ".format(key)
            cmd = cmd[:-2]
            cmd += " FROM {}".format(table)
            rdata = self.__disassemble_return_value(self.__use_db(cmd).fetchall(), table)
            self.__logger.debug("fetchall data: {}".format(rdata))
            self.__data.update({table: rdata})
        if table_name:
            return self.__data.get(table_name, {})
        else:
            return self.__data

    def __use_db(self, cmd, data=False):
        """Execute SQL commands
            :param cmd: SQL command
            :type cmd: str
            :param data: data to insert into db
            :type data: tuple
            :return: data from db
        """
        if not data:
            self.__logger.debug("execute: {}".format(cmd))
            return_value = self.__curser.execute(cmd)
        else:
            self.__logger.debug("execute: {}, {}".format(cmd, data))
            return_value = self.__curser.execute(cmd, data)
        self.__db_conn.commit()
        return return_value

    def del_data(self, del_data_dict, table) -> None:
        """delete data entry from table
            :param del_data_dict: data entry to delete
            :type del_data_dict: dict
            :param table: table name to delete from
            :type table: str
        """
        del_key = list(del_data_dict.keys())[0]
        if del_key in self.__data.get(table, {}).keys():
            self.__logger.debug("key exists - delete")
            primary = self.__primary.get(table, False)
            cmd_str = "DELETE FROM {} WHERE ".format(table)
            if type(primary) is not list:
                cmd_str += "{}=?".format(primary)
                del_key = (del_key,)
            else:
                del_key_list = []
                for i in range(len(primary)):
                    cmd_str += "{}=? AND ".format(primary[i])
                    del_key_list.append(del_key[i])
                del_key = tuple(del_key_list)
                cmd_str = cmd_str[:-5]
            self.__use_db(cmd_str, del_key)

    def set_data(self, new_data, table):
        """Make new entry into table
            :param new_data: new data
            :type new_data: dict
            :param table: table name
            :type table: str
            :return: returns true or false
            :rtype: bool
        """
        if new_data == {}:
            self.__logger.info("Primary key {} for table {} not set in new data".format(self.__primary.get(table,
                                                                                                           False), table))
            return False
        else:
            if self.__data.get(table, {}) == {}:
                self.get_data()
            new_key = list(new_data.keys())[0]
            new_value = list(new_data.values())[0]
            self.__logger.debug("delete entry if entry with new key already exists")
            existing_data = copy.deepcopy(self.__data.get(table, {}).get(new_key, {}))
            self.del_data(new_data, table)
            self.__logger.debug("insert entry {}")
            insert_primary_str, insert_primary_value_str, insert_primary_value = self.__insert_primary(table, new_key)
            insert_str = "INSERT INTO {} ({}".format(table, insert_primary_str)
            value_str = " VALUES ({}".format(insert_primary_value_str)
            values = insert_primary_value
            for key in self.__keys.get(table, []):
                if key not in self.__primary.get(table, False):
                    insert_str += "{}, ".format(key)
                    value_str += "?, "
                    if key in new_value:
                        values.append(new_value.get(key))
                    else:
                        values.append(existing_data.get(key, ""))
            cmd_str = "{}){})".format(insert_str[:-2], value_str[:-2])
            values = tuple(values)
            self.__use_db(cmd_str, values)
        return True

    def __insert_primary(self, table, new_key):
        """creates insert strings and values dependent on if table uses single or multi--column primary key
            :param new_key: new data
            :type new_key: dict
            :param table: table name
            :type table: str
            :return: returns needed SQL statement parts for column name str, value string, value list
            :rtype: str, str, list
        """
        primary = self.__primary.get(table, False)
        return_value = []
        return_string_name = ""
        return_string_value = ""
        if type(primary) is list:
            if len(primary) != len(new_key):
                raise ValueError("missmatch key length")
            for i in range(len(primary)):
                return_string_value += "?, "
                return_string_name += "{}, ".format(primary[i])
                return_value.append(new_key[i])
        else:
            return_string_value += "?, "
            return_string_name += "{}, ".format(primary)
            return_value.append(new_key)
        return return_string_name, return_string_value, return_value

    def close(self):
        """Close the connection to db"""
        self.__db_conn.close()


if __name__ == "__main__":
    path_dir = "/tmp"
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG)

    access_db_layout = {"user": {"uid": "TEXT NOT NULL PRIMARY KEY", "name": "TEXT NOT NULL", "create_date": "TEXT"},
                        "lock": {"mac": "TEXT NOT NULL PRIMARY KEY", "name": "TEXT NOT NULL", "create_date": "TEXT",
                                 "last_seen": "TEXT"},
                        "access": {"lock": "TEXT NOT NULL PRIMARY KEY", "user": "TEXT NOT NULL PRIMARY KEY",
                                   "access": "BOOL"},
                        "log": {"uid": "TEXT NOT NULL PRIMARY KEY", "date": "TEXT", "user": "TEXT NOT NULL",
                                "lock": "TEXT NOT NULL", "accessed": "INTEGER"}}
    access_db_filename = os.path.join(path_dir, "example_access.db")
    access_db = VaultDBDict(access_db_filename, access_db_layout)

    logger.info("Access DB")

    # users
    new_user = {"id_1": {"test": "test stuff"}}
    access_db.set_data(new_user, table="user")
    logger.info("users: {}".format(access_db.get_data("user")))
    time_str = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    new_user = {"id_1": {"create_date": time_str, "name": "Hans"}}
    access_db.set_data(new_user, table="user")
    logger.info("users: {}".format(access_db.get_data("user")))

    # locks
    logger.info("locks: {}".format(access_db.get_data("lock")))
    new_lock = {"mac_address_1": {"name": "front door", "create_date": "2024-09-24", "last_seen": "2024-10-01"}}
    access_db.set_data(new_lock, table="lock")
    new_lock = {"mac_address_2": {"name": "garage door", "create_date": "2024-09-24", "last_seen": "2024-10-01"}}
    access_db.set_data(new_lock, table="lock")
    logger.info("locks: {}".format(access_db.get_data("lock")))

    # access rights
    logger.info("access rights: {}".format(access_db.get_data("access")))
    new_access = {("id_1", "mac_address_1"): {"access": True}}
    access_db.set_data(new_access, table="access")
    new_access = {("id_1", "mac_address_2"): {"access": False}}
    access_db.set_data(new_access, table="access")
    logger.info("access rights: {}".format(access_db.get_data("access")))

    # logs
    import time
    import uuid
    import pprint
    new_log_entry = {str(uuid.uuid4()): {"user": "id_1", "lock": "mac_address_2",
                                         "date": str(time.time()), "accessed": 5}}
    access_db.set_data(new_log_entry, table="log")

    print("complete db json print - with pprint for better readability")
    pprint.pprint(access_db.get_data())
    print("closing db connection")
    access_db.close()
