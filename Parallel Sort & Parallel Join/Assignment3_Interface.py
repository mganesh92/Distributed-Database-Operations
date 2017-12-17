#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import threading
import os
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
NUMBER_OF_THREADS = 5
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    cursor = openconnection.cursor()        
    cursor.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + "")
    minVal = cursor.fetchone()[0]    
    cursor.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + "")
    maxVal = cursor.fetchone()[0]
    interval = (maxVal - minVal) / NUMBER_OF_THREADS    
    inputTableSchema = inputSchema(InputTable, openconnection)    
    for i in range(NUMBER_OF_THREADS):
        tableName = "rangePart" + str(i)
        tableCreate(tableName, inputTableSchema, openconnection)    
    threads = [0, 0, 0, 0, 0]
    for i in range(NUMBER_OF_THREADS):
        if i == 0:
            minLimit = minVal
            maxLimit = minVal + interval
        else:
            minLimit = maxLimit
            maxLimit += interval
        threads[i] = threading.Thread(target=insertion,args=(InputTable, SortingColumnName, i, minLimit, maxLimit, openconnection))
        threads[i].start()
    for i in range(NUMBER_OF_THREADS):
        threads[i].join()    
    tableCreate(OutputTable, inputTableSchema, openconnection)
    for i in range(NUMBER_OF_THREADS):            
        cursor.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + "rangePart" + str(i) + "")
    for i in range(NUMBER_OF_THREADS):        
        cursor.execute("DROP TABLE IF EXISTS " + "rangePart" + str(i) + "")
    openconnection.commit()
    #Implement ParallelSort Here.
    # pass #Remove this once you are done with implementation



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cursor = openconnection.cursor()    
    cursor.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    result = cursor.fetchone()
    mintable1 = float(result[0])    
    cursor.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    result = cursor.fetchone()
    mintable2 = float(result[0])    
    cursor.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    result = cursor.fetchone()
    maxtable1 = float(result[0])    
    cursor.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    result = cursor.fetchone()
    maxtable2 = float(result[0])
    globalmax = max(maxtable1, maxtable2)
    globalmin = min(mintable1, mintable2)
    interval = (globalmax - globalmin) / NUMBER_OF_THREADS    
    inputSchemaTable1 = inputSchema(InputTable1, openconnection)
    inputSchemaTable2 = inputSchema(InputTable2, openconnection)    
    createInnerJoinTable(OutputTable, inputSchemaTable1, inputSchemaTable2, openconnection)    
    rangeTablesParallel(InputTable1, Table1JoinColumn, interval, globalmin, "ipTable1range", openconnection)
    rangeTablesParallel(InputTable2, Table2JoinColumn, interval, globalmin, "ipTable2range", openconnection)    
    for i in range(NUMBER_OF_THREADS):
        outputRangeTableName = "opTableRange" + str(i)
        createInnerJoinTable(outputRangeTableName, inputSchemaTable1, inputSchemaTable2, openconnection)    
    threads = [0, 0, 0, 0, 0]
    for i in range(NUMBER_OF_THREADS):
        threads[i] = threading.Thread(target=parallelJoinInsertion, args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
        threads[i].start()    
    for i in range(NUMBER_OF_THREADS):
        threads[i].join()    
    for i in range(NUMBER_OF_THREADS):            
        cursor.execute("INSERT INTO " + OutputTable + " SELECT * FROM " + "opTableRange" + str(i) + "")
    for i in range(NUMBER_OF_THREADS):        
        cursor.execute("DROP TABLE IF EXISTS ipTable1range" + str(i) + "")
        cursor.execute("DROP TABLE IF EXISTS ipTable2range" + str(i) + "")
        cursor.execute("DROP TABLE IF EXISTS opTableRange" + str(i) + "")        
    openconnection.commit()
    #Implement ParallelJoin Here.
    # pass # Remove this once you are done with implementation

def tableCreate(tableName, inputTableSchema, openconnection):
    cursor = openconnection.cursor()    
    cursor.execute("DROP TABLE IF EXISTS " + tableName + "")    
    cursor.execute("CREATE TABLE " + tableName + " ("+inputTableSchema[0][0]+" "+inputTableSchema[0][1]+")")
    for j in range(1, len(inputTableSchema)):        
        cursor.execute("ALTER TABLE " + tableName + " ADD COLUMN " + inputTableSchema[j][0] + " " + inputTableSchema[j][1] + ";")

def insertion(InputTable, SortingColumnName, i, minLimit, maxLimit, openconnection):
    cursor = openconnection.cursor()
    tableName = "rangePart" + str(i)
    if i == 0:        
        query = "INSERT INTO " + tableName + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(minLimit) + " AND " + SortingColumnName + " <= " + str(maxLimit) + " ORDER BY " + SortingColumnName + " ASC"
    else:        
        query = "INSERT INTO " + tableName + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(minLimit) + " AND " + SortingColumnName + " <= " + str(maxLimit) + " ORDER BY " + SortingColumnName + " ASC"    
    cursor.execute(query)
    return

def inputSchema(InputTable, openconnection):
    cursor = openconnection.cursor()    
    cursor.execute("SELECT column_name,data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name='" + InputTable + "'")
    inputTableSchema = cursor.fetchall()
    return inputTableSchema

def createInnerJoinTable(tableName, inputTableSchema1, inputTableSchema2, openconnection):
    cursor = openconnection.cursor()    
    cursor.execute("DROP TABLE IF EXISTS " + tableName + "")    
    cursor.execute("CREATE TABLE " + tableName + " ("+inputTableSchema1[0][0]+" "+inputTableSchema2[0][1]+")")
    for j in range(1, len(inputTableSchema1)):        
        cursor.execute("ALTER TABLE " + tableName + " ADD COLUMN " + inputTableSchema1[j][0] + " " + inputTableSchema1[j][1] + ";")
    for j in range(len(inputTableSchema2)):        
        cursor.execute("ALTER TABLE " + tableName + " ADD COLUMN " + inputTableSchema2[j][0] + " " + inputTableSchema2[j][1] + ";")

def rangeTablesParallel(InputTable, TableJoinColumn, interval, globalmin, tempTable, openconnection):
    cursor = openconnection.cursor()
    for i in range(NUMBER_OF_THREADS):
        tableName = tempTable + str(i)       
        cursor.execute("DROP TABLE IF EXISTS " + tableName + "") 
        if i == 0:
            min = globalmin
            max = globalmin+interval            
            query = "CREATE TABLE " + tableName + " AS SELECT * FROM " + InputTable + " WHERE (" + TableJoinColumn + " >= " + str(min) + ") AND (" + TableJoinColumn + " <= " + str(max) + ");"
        else:
            min = max
            max = max+interval            
            query = "CREATE TABLE " + tableName + " AS SELECT * FROM " + InputTable + " WHERE (" + TableJoinColumn + " > " + str(min) + ") AND (" + TableJoinColumn + " <= " + str(max) + ");"               
        cursor.execute(query)

def parallelJoinInsertion(Table1JoinColumn, Table2JoinColumn, openconnection, i):
    cursor = openconnection.cursor()    
    query = "INSERT INTO opTableRange" + str(i) + " SELECT * FROM ipTable1range" + str(i) + " INNER JOIN ipTable2range" + str(i) + " ON ipTable1range" + str(i) + "." + Table1JoinColumn + "=" + "ipTable2range" + str(i) + "." + Table2JoinColumn + ";"
    cursor.execute(query)
    return


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
