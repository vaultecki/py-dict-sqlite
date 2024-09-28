import copy
import datetime
import os
import sqlite3 as sqlite
import uuid


class VaultDBDict:
    def __init__(self, filename, db_layout, password="none", debug=False):
        """Try to smooth out handling sqlite as dict with get and set methods
            :param filename: name of the sql file
            :type filename: str
            :param db_layout: db layout to be created
            :type db_layout: dict
            :param password: password for database protection, optional
            :type password: str

        """
        self.debug = debug
        # check if db file exist
        self.db_exists = os.path.exists(str(filename))
        self.db_layout = db_layout
        self.filename = str(filename)
        self.keys = {}
        self.primary = {}
        self.tables = []
        self.data = {}
        self.analyse_db_from_json()
        self.__print("opening db {}".format(self.filename))
        self.db_conn = sqlite.connect(self.filename, check_same_thread=False)
        self.curser = self.db_conn.cursor()
        if not self.db_exists:
            self.__print("db {} does not exist - creating".format(filename))
            self.create_tables()
        self.get_data()

    def __print(self, text):
        if self.debug:
            print(text)

    def create_db_string_from_json(self, table, json_str):
        """Create table using given parameter
            :param table: name of the db table
            :type table: str
            :param json_str: structure of table
            :type json_str: dict
            :return: SQL command
            :rtype: str
        """
        self.__print("create table {}".format(table))
        db_str = "CREATE TABLE {} (".format(table)
        for key in json_str:
            attribute = json_str.get(key)
            self.__print("table: {}; key: {}; value: {}".format(table, key, attribute))
            if type(attribute) == dict:
                for attribute in json_str.get(key, {}):
                    table_str = json_str.get(key, {})
                    db_str += self.create_table_column(attribute, table_str.get(attribute), table)
            else:
                db_str += self.create_table_column(key, attribute, table)
        db_str = db_str[:-1] + self.create_table_primary(table) + ")"
        return db_str

    def create_table_primary(self, table):
        primary = self.primary.get(table, False)
        return_string = ""
        if type(primary) == list:
            return_string = ", PRIMARY KEY ("
            for element in primary:
                return_string += "{}, ".format(element)
            return_string = return_string[:-2] + ")"
        return return_string

    def create_table_column(self, column, attribute, table):
        # " {} {},".format(attribute, table_str.get(attribute))
        primary = self.primary.get(table, False)
        attribute = attribute.upper()
        if type(primary) == list:
            if "PRIMARY KEY" in attribute:
                attribute = attribute.replace(" PRIMARY KEY", "")
                if "NOT NULL" not in attribute:
                    attribute += " NOT NULL"
        return_string = " {} {},".format(column, attribute)
        return return_string

    def analyse_db_from_json(self):
        """Read the given parameters, search for primary key"""
        self.__print("analyse db layout from json")
        for table in self.db_layout:
            self.tables.append(table)
            keys = []
            for key, value in self.db_layout.get(table).items():
                keys.append(key)
                self.__print("table: {}; key: {}; value: {}".format(table, key, value))
                if "PRIMARY KEY" in value:
                    primary = self.primary.get(table, False)
                    if not primary:
                        primary = key
                    else:
                        if type(primary) != list:
                            primary = [primary]
                        primary.append(key)
                    self.primary.update({table: primary})
            self.__print("primary key(s) for table {} are {}".format(table, self.primary.get(table, False)))
            self.keys.update({table: keys})

    def create_tables(self):
        """Create tables for all layouts"""
        for table in self.db_layout:
            create_str = self.create_db_string_from_json(table, self.db_layout.get(table, ""))
            self.__print("create string for db: {}".format(create_str))
            self.use_db(create_str)

    def disassemble_return_value(self, rdata, table):
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
            primary = self.primary.get(table, False)
            if not primary:
                return data
            entry_layout = self.db_layout.get(table)
            for new_data in rdata:
                start_data = 1
                new_key = new_data[0]
                if type(primary) == list:
                    start_data = len(primary)
                    new_key = []
                    for i in range(start_data):
                        new_key.append(new_data[i])
                    new_key = tuple(new_key)
                new_dict = {}
                for key in self.keys.get(table):
                    if key not in primary:
                        new_dict.update({key: new_data[start_data]})
                        start_data += 1
                data.update({new_key: new_dict})
        return data

    def get_data(self, table_name=False):
        """Get data from table
            :param table_name: table name to be searched
            :type table_name: str
            :return: got tables
            :rtype: dict
        """
        self.__print("getdata")
        for table in self.db_layout:
            cmd = "SELECT "
            primary = self.primary.get(table, False)
            if type(primary) == list:
                for element in primary:
                    cmd += "{}, ".format(element)
            else:
                cmd += "{}, ".format(primary)
            for key in self.keys.get(table):
                if key not in primary:
                    cmd += "{}, ".format(key)
            cmd = cmd[:-2]
            cmd += " FROM {}".format(table)
            rdata = self.disassemble_return_value(self.use_db(cmd).fetchall(), table)
            self.__print("fetchall data: {}".format(rdata))
            self.data.update({table: rdata})
        if table_name:
            return self.data.get(table_name, {})
        else:
            return self.data

    def use_db(self, cmd, data=False):
        """Execute SQL commands
            :param cmd: SQL command
            :type cmd: str
            :param data: data to insert into db
            :type data: tuple
            :return: data from db
        """
        if not data:
            self.__print("execute: {}".format(cmd))
            return_value = self.curser.execute(cmd)
        else:
            self.__print("execute: {}, {}".format(cmd, data))
            return_value = self.curser.execute(cmd, data)
        self.db_conn.commit()
        return return_value

    def del_data(self, del_data_dict, table):
        # print(del_data_dict)
        # {"single_primary_key": True}
        # {("primary_key1", "primary_key_2"): True}
        del_key = list(del_data_dict.keys())[0]
        if del_key in self.data.get(table, {}).keys():
            self.__print("data exists - delete first")
            primary = self.primary.get(table, False)
            cmd_str = "DELETE FROM {} WHERE ".format(table)
            if type(primary) != list:
                cmd_str += "{}=?".format(primary)
                del_key = (del_key,)
            else:
                del_key_list = []
                for i in range(len(primary)):
                    cmd_str += "{}=? AND ".format(primary[i])
                    del_key_list.append(del_key[i])
                del_key = tuple(del_key_list)
                cmd_str = cmd_str[:-5]
            self.use_db(cmd_str, del_key)

    def set_data(self, new_data, table):
        """Make new entry into table
            :param new_data: new data
            :type new_data: dict
            :param table: table name
            :type table: str
            :return: returns 0
            :rtype: int
        """
        if new_data == {}:
            self.__print("Primary key {} for table {} not set in new data".format(self.primary.get(table, False), table))
        else:
            if self.data.get(table, {}) == {}:
                self.get_data()
            existing_data = {}
            new_key = list(new_data.keys())[0]
            new_value = list(new_data.values())[0]
            self.__print("delete")
            existing_data = copy.deepcopy(self.data.get(table, {}).get(new_key, {}))
            self.del_data(new_data, table)
            self.__print("insert")
            insert_primary_str, insert_primary_value_str, insert_primary_value = self.insert_primary(table, new_key)
            insert_str = "INSERT INTO {} ({}".format(table, insert_primary_str)
            value_str = " VALUES ({}".format(insert_primary_value_str)
            values = insert_primary_value
            for key in self.keys.get(table, []):
                if key not in self.primary.get(table, False):
                    insert_str += "{},".format(key)
                    value_str += "?,"
                    if key in new_value:
                        values.append(new_value.get(key))
                    else:
                        values.append(existing_data.get(key, ""))
            cmd_str = "{}){})".format(insert_str[:-1], value_str[:-1])
            values = tuple(values)
            self.use_db(cmd_str, values)
        return 0

    def insert_primary(self, table, new_key):
        primary = self.primary.get(table, False)
        return_value = []
        return_string_name = ""
        return_string_value = ""
        if type(primary) == list:
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
        self.db_conn.close()


if __name__ == "__main__":
    path_dir = "/tmp"
    # os.getcwd()  # "C:\\Temp"

    db_layout = {"key_user": {"uid": "TEXT NOT NULL PRIMARY KEY", "name": "TEXT NOT NULL", "create_date": "TEXT"},
                 "lock": {"mac": "TEXT NOT NULL PRIMARY KEY", "name": "TEXT NOT NULL", "create_date": "TEXT", "last_seen": "TEXT"},
                 "access": {"lock": "TEXT NOT NULL PRIMARY KEY", "key_user": "TEXT NOT NULL PRIMARY KEY"},
                 "web_user": {"uuid": "TEXT NOT NULL PRIMARY KEY", "password": "TEXT NOT NULL", "name": "TEXT NOT NULL", "role": "TEXT", "create_date": "TEXT"},
                 "log": {"uuid": "TEXT NOT NULL PRIMARY KEY", "date": "TEXT", "key_user": "TEXT NOT NULL", "lock": "TEXT NOT NULL", "accessed": "INTEGER"}}
    user_db_filename = os.path.join(path_dir, "lock_user.db")
    user_db = VaultDBDict(user_db_filename, db_layout, debug=True)

    print("User DB")
    print(user_db.get_data("key_user"))

    new_user = {"id_1": {"name": "Hans", "create_date": "admin"}}
    user_db.set_data(new_user, "key_user")
    print(user_db.get_data("key_user"))
    time_str = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    new_user = {"1": {"create_date": time_str, "name": "admin"}}
    user_db.set_data(new_user, "user")
    print(user_db.get_data())

    test_db_layout = {"access": {"lid": "TEXT NOT NULL PRIMARY KEY", "uid": "TEXT NOT NULL PRIMARY KEY", "access": "BOOL"}}
    test_db_filename = os.path.join(path_dir, "test.db")
    test_db = VaultDBDict(test_db_filename, test_db_layout, debug=True)
    print("Test DB")
    print("test_db entries: {}".format(test_db.get_data("access")))
    new_door = {("2", "1"): {"access": True}}
    test_db.set_data(new_door, "access")
