#!/usr/bin/env python

import sqlite3
import csv
import argparse

class CsvToSqlite(object):
    @staticmethod
    def createSQLite(csvFile, dbFile, tableName):
        with open(csvFile, 'r') as f:
            dialect = csv.Sniffer().sniff(f.read(1024))
            f.seek(0)
            csvReader = csv.DictReader(f, delimiter = dialect.delimiter)

            # create sqlite db
            conn = sqlite3.connect(dbFile)
            c = conn.cursor()

            # Create a table creation query
            # Get the first row, and base the data types for the columns on this row
            row = csvReader.next()

            keys = row.keys()
            fielddefs = []
            for key in keys:
                if key == None:
                    raise Exception('The number of column names is not equal to the number of columns.')
                if key == 'id':
                    raise Exception("""Please pick a different name for the "id" column.
                                       It is used interally by this script to generate a unique
                                       identifier for each row.""")
                if row[key].isdigit():
                    coltype = 'INTEGER'
                elif CsvToSqlite.is_float(row[key]):
                    coltype = 'REAL'
                else:
                    coltype = 'TEXT'
                fielddefs.append('"%s" %s' % (key, coltype))

            q = 'CREATE TABLE IF NOT EXISTS "%s" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, %s)' % (tableName, ','.join(fielddefs))
            if not sqlite3.complete_statement(q):
                err = 'Query %s is not valid. This may mean you are using reserved words for the column or table names.' % q
                raise Exception(err)
            c.execute(q)

            insertQuery = 'INSERT INTO %s (%s) VALUES (%s)' % (tableName, ','.join(keys), ','.join([':' + x for x in keys]));

            c.execute(insertQuery, row)
            for row in csvReader:
                c.execute(insertQuery, row)

            print('\tWriting %s' % dbFile)
            conn.commit()

    @staticmethod
    def is_float(var):
        try:
            float(var)
            return True
        except ValueError:
            return False

    @staticmethod
    def parseArguments():
        parser = argparse.ArgumentParser(description="""Convert csv file to an SQLite3 database""")
        parser.add_argument('--csv',   type=str, dest='csvFile',    required=True, help='Input csv.')
        parser.add_argument('--db',    type=str, dest='dbFile',     required=True, help='Output sqlite file.')
        parser.add_argument('--table', type=str, dest='tableName',  required=True, help='Name of the database table to create.')
        return parser.parse_args()

if __name__ == '__main__':
    args = CsvToSqlite.parseArguments()
    CsvToSqlite.createSQLite(args.csvFile, args.dbFile, args.tableName)
