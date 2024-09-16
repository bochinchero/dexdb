# this routine handles data collection, it uses sqlite3 to store the data into a file
import sqlite3, os
import logging
import pandas as pd
logger = logging.getLogger(__name__)

# logic to check the paths are valid, etc.
def pathCheck(path):
    try:
        # verify isf file already exists
        if os.path.isfile(path):
            logger.debug(f'file already exists {path}')
            return
        else:
            # verify if folder does not exist
            folderPath = os.path.dirname(path)
            if not os.path.isdir(folderPath):
                # make the folder
                os.makedirs(folderPath)
                logger.info(f' created folder {folderPath}')
            return
    except os.error as error:
        logger.error(f'{error} for {path} ')
        return None


# this function deals with connecting to the database, will return none if the connection failed
def dbConnect(path):
    try:
        # ensure the path is valid
        pathCheck(path)
        # connect to the database file
        con = sqlite3.connect(path)
        # get cursor
        cur = con.cursor()
        logger.debug(f'connected to {path} ')
        return con, cur
    except sqlite3.Error as error:
        logger.error(f'{error} for {path} ')
        return None, None


# this function creates the specified table with the specified columns-
def createTable(cur,tableName,listCols):
    try:
        # verify if the table already exists
        results = cur.execute(f'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'{tableName}\'')
        if results.fetchone() is not None:
            logger.debug(f'verified table {tableName} exists.')
            return True
        else:
            # declare the empty column string
            colStr = ''
            # add all columns
            for item in listCols:
                colStr = colStr + item + ', '
            # build the command string
            commandStr = 'CREATE TABLE IF NOT EXISTS ' + tableName + ' ( ' + colStr[:-2] + ' ) '
            # execute
            cur.execute(commandStr)
            logger.info(f'table {tableName} created.')
            return True
    except sqlite3.Error as error:
        logger.error(f'{error}')
        return False


# this function connects to a database, and sets up the required table structure if it doesn't exist
def initalizeDB(path):
    try:
        logger.info(f'starting database initialization {path}')
        # connect to database
        conn, cur = dbConnect(path)
        # create exchanges table
        result = createTable(cur, 'exchanges', ['ID integer primary key',
                                                'name varchar(30) not null',
                                                'unique (name)'])
        if not result:
            raise Exception("exchanges table can't be created")
        # create markets table
        result = createTable(cur, 'markets', ['ID integer primary key',
                                              'exchangeID integer not null',
                                              'name varchar(30) not null',
                                              'base varchar(30) not null',
                                              'quote varchar(30) not null',
                                              'unique (exchangeID, name, base, quote)'
                                              ])
        if not result:
            raise Exception("markets table can't be created")
        # create market config table
        result = createTable(cur, 'marketConfig', ['marketID int not null',
                                                   'epochlen int not null',
                                                   'lotsize bigint not null',
                                                   'parcelSize bigint not null',
                                                   'ratestep int not null',
                                                   'baseConversionFactor bigint not null',
                                                   'quoteConversionFactor bigint not null',
                                                   'LastUpdated timestamp DATETIME not null',
                                                   'unique (marketID)'
                                                   ])
        if not result:
            raise Exception("markets table can't be created")
        # create books table
        result = createTable(cur, 'books', ['marketID integer not null',
                                            'TimeStamp datetime not null',
                                            'side integer not null',
                                            'rate bigint not null',
                                            'qty bigint not null'])
        if not result:
            raise Exception("books table can't be created")
        result = createTable(cur, 'candles', ['marketID integer not null',
                                              'timeOpen datetime not null',
                                              'timeClose datetime not null',
                                              'baseVolume bigint not null',
                                              'quoteVolume bigint not null',
                                              'high bigint not null',
                                              'low bigint not null',
                                              'open bigint not null',
                                              'close bigint not null',
                                              'unique (marketID, timeOpen)'])
        if not result:
            raise Exception("candles table can't be created")
        conn.commit()
        conn.close()
        return True
    except Exception as err:
        logger.error(f'{err=}, {type(err)=}')
        conn.close()
        raise


def insertRecords(path,tableName,inputData, colDict, replace=False):
    try:
        # connect to database
        conn, cur = dbConnect(path)
        # create a filtered copy of input data only with the specified columns and renaming them as required
        # colDict is a dictionary where the keys are the input data columns and the values the
        filteredData = pd.DataFrame()
        filteredColsStr = ''

        for inputCol in colDict:
            outputCol = colDict[inputCol]
            filteredData[outputCol] = inputData[inputCol]
            # this string is used for the column list
            filteredColsStr += outputCol + ' '
        # remove the last comma and space
        filteredColsStr = filteredColsStr[:-1].replace(' ', ', ')
        # create a temp table for inserting results
        filteredData.to_sql('tempTable', conn, if_exists='replace',index=False)
        # handle the replace case
        if replace is False:
            cmdStr = 'INSERT OR IGNORE'
        else:
            cmdStr = 'INSERT OR REPLACE'

        # build string
        queryStr = f'{cmdStr} INTO {tableName} ( {filteredColsStr} ) SELECT * FROM tempTable as tempTable'
        # execute
        result = conn.execute(queryStr)
        if result is None:
            raise Exception(f'data could not be updated for {tableName}')
        # drop temp table
        conn.execute('DROP TABLE tempTable')
        # commit and close
        conn.commit()
        conn.close()
    except Exception as err:
        logger.error(f'{err=}, {type(err)=}')
        conn.close()
        raise


def readTable(path,tableName,listCols=None,whereClause=None):
    try:
        # connect to database
        conn, cur = dbConnect(path)
        # create a default column list if it isn't provided
        if listCols is None:
            listColStr = '*'
        else:
            # if it is provided, parse the list into the correct format
            listColStr = ''
            for col in listCols:
                listColStr += col + ' '
            listColStr = listColStr[:-1].replace(' ', ', ')
        # add a clause for filtering data at the query level
        if whereClause is None:
            whereStr = ''
        else:
            whereStr = f'WHERE {whereClause}'
        # build the sql query
        queryStr = f'Select {listColStr} from {tableName} {whereStr}'
        # execute
        data = pd.read_sql_query(queryStr, conn)
        # close
        conn.close()
        # return data
        return data
    except Exception as err:
        logger.error(f'{err=}, {type(err)=}')
        conn.close()
        raise


# this function is an open/free query into the specified database
def freeQuery(path,queryStr):
    try:
        # connect to database
        conn, cur = dbConnect(path)
        # create a default column list if it isn't provided
        # execute
        data = pd.read_sql_query(queryStr, conn)
        # close
        conn.close()
        # return data
        return data
    except Exception as err:
        logger.error(f'{err=}, {type(err)=}')
        raise