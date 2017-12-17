#!/usr/bin/python2.7
#
# Assignment2 Interface
#

__author__ = 'Nikhil Lohia'

import psycopg2
import os
import sys
from psycopg2.extensions import AsIs

DATABASE_NAME = 'dds_assgn1'
RANGE_TABLE_PREFIX = 'RangeRatingsPart'
RANGE_TABLE_PREFIX_PSQL = 'rangeratingspart'
RROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'
RROBIN_TABLE_PREFIX_PSQL = 'roundrobinratingspart'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
RANGE_QUERY_OUTPUT_FILE = 'RangeQueryOut.txt'
POINT_QUERY_OUTPUT_FILE = 'PointQueryOut.txt'


# Method returns false if a table already exists in the database
def checktableexists(openconnection, tablename):
    cursor = openconnection.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if cursor.fetchone()[0] == 1:
        cursor.close()
        return True

    cursor.close()
    return False


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursor = openconnection.cursor()

    # Implement RangeQuery Here.

    partitiontable = 0
    rangetablename = RANGE_TABLE_PREFIX_PSQL + repr(partitiontable)
    printtablename = RANGE_TABLE_PREFIX + repr(partitiontable)
    while True:
        if not checktableexists(openconnection, rangetablename):
            break

        query = "SELECT * FROM %s WHERE rating >= %s AND rating <= %s;"
        data = (AsIs(rangetablename), ratingMinValue, ratingMaxValue)
        cursor.execute(query, data)
        rows = cursor.fetchall()

        with open(RANGE_QUERY_OUTPUT_FILE, 'a+') as f:
            for row in rows:
                f.write("%s," % printtablename)
                f.write("%s," % str(row[0]))
                f.write("%s," % str(row[1]))
                f.write("%s\n" % str(row[2]))
        f.close()

        partitiontable += 1
        rangetablename = RANGE_TABLE_PREFIX_PSQL + repr(partitiontable)
        printtablename = RANGE_TABLE_PREFIX + repr(partitiontable)

    partitiontable = 0
    rrobintablename = RROBIN_TABLE_PREFIX_PSQL + repr(partitiontable)
    printtablename = RROBIN_TABLE_PREFIX + repr(partitiontable)
    while True:
        if not checktableexists(openconnection, rrobintablename):
            break

        query = "SELECT * FROM %s WHERE rating >= %s AND rating <= %s;"
        data = (AsIs(rrobintablename), ratingMinValue, ratingMaxValue)
        cursor.execute(query, data)
        rows = cursor.fetchall()

        with open(RANGE_QUERY_OUTPUT_FILE, 'a+') as f:
            for row in rows:
                f.write("%s," % printtablename)
                f.write("%s," % str(row[0]))
                f.write("%s," % str(row[1]))
                f.write("%s\n" % str(row[2]))
        f.close()

        partitiontable += 1
        rrobintablename = RROBIN_TABLE_PREFIX_PSQL + repr(partitiontable)
        printtablename = RROBIN_TABLE_PREFIX + repr(partitiontable)


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()

    # Implement PointQuery Here.

    partitiontable = 0
    rangetablename = RANGE_TABLE_PREFIX_PSQL + repr(partitiontable)
    printtablename = RANGE_TABLE_PREFIX + repr(partitiontable)
    while True:
        if not checktableexists(openconnection, rangetablename):
            break

        query = "SELECT * FROM %s WHERE rating = %s;"
        data = (AsIs(rangetablename), ratingValue)
        cursor.execute(query, data)
        rows = cursor.fetchall()

        with open(POINT_QUERY_OUTPUT_FILE, 'a+') as f:
            for row in rows:
                f.write("%s," % printtablename)
                f.write("%s," % str(row[0]))
                f.write("%s," % str(row[1]))
                f.write("%s\n" % str(row[2]))
        f.close()

        partitiontable += 1
        rangetablename = RANGE_TABLE_PREFIX_PSQL + repr(partitiontable)
        printtablename = RANGE_TABLE_PREFIX + repr(partitiontable)

    partitiontable = 0
    rrobintablename = RROBIN_TABLE_PREFIX_PSQL + repr(partitiontable)
    printtablename = RROBIN_TABLE_PREFIX + repr(partitiontable)
    while True:
        if not checktableexists(openconnection, rrobintablename):
            break

        query = "SELECT * FROM %s WHERE rating = %s;"
        data = (AsIs(rrobintablename), ratingValue)
        cursor.execute(query, data)
        rows = cursor.fetchall()

        with open(POINT_QUERY_OUTPUT_FILE, 'a+') as f:
            for row in rows:
                f.write("%s," % printtablename)
                f.write("%s," % str(row[0]))
                f.write("%s," % str(row[1]))
                f.write("%s\n" % str(row[2]))
        f.close()

        partitiontable += 1
        rrobintablename = RROBIN_TABLE_PREFIX_PSQL + repr(partitiontable)
        printtablename = RROBIN_TABLE_PREFIX + repr(partitiontable)
