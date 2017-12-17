#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    # pass #Remove this once you are done with implementation
    print "start range partition"
    cursor=openconnection.cursor()    
    f=open('RangeQueryOut.txt', 'w')    
    cursor.execute("Select PartitionNum from RangeRatingsMetadata where "+repr(ratingMinValue)+"<=minrating or "+ repr(ratingMaxValue)+">=maxrating")
    Values=cursor.fetchall()
    for index in Values:                
        cursor.execute("Select * from "+"RangeRatingsPart" + repr(index[0])+" where Rating>="+repr(ratingMinValue)+" AND Rating <="+repr(ratingMaxValue))
        rows = cursor.fetchall()        
        for value in rows:            
            f.write("RangeRatingsPart" + repr(index[0]) + "," + repr(value[0]) + "," + repr(value[1]) + "," + repr(value[2]))
            f.write("\n")

    cursor.execute("Select Partitionnum from RoundRobinRatingsMetadata")
    robin_count = cursor.fetchall()
    for value in range(0,robin_count[0][0]):
        tableName="RoundRobinRatingsPart"+repr(value)
        cursor.execute("Select * from "+tableName+" where Rating>="+repr(ratingMinValue)+" AND Rating <="+repr(ratingMaxValue))
        rows=cursor.fetchall()
        for value in rows:
            f.write(tableName +","+ repr(value[0]) +","+ repr(value[1]) +","+ repr(value[2]) + "\n")    
    f.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    # pass # Remove this once you are done with implementation
    print "start point query"
    cursor = openconnection.cursor()    
    range_name = "RangeRatingsPart"
    rrobin_name = "RoundRobinRatingsPart"
    f = open('PointQueryOut.txt', 'w')
    cursor.execute("Select PartitionNum from RangeRatingsMetadata where " + repr(ratingValue) + ">=minrating and " + repr(ratingValue) + "<=maxrating")
    rangeVal = cursor.fetchall()    
    for Value in rangeVal:
        Name = range_name + repr(Value[0])
        cursor.execute("Select * from " + Name + " where Rating =  "+ str(ratingValue))
        rows = cursor.fetchall()
        for row in rows:
            f.write(Name + "," + repr(row[0]) + "," + repr(row[1]) + "," + repr(row[2]) + "\n")
    cursor.execute("Select Partitionnum from RoundRobinRatingsMetadata")
    robin_count = cursor.fetchall()
    for each in range(0,robin_count[0][0]):
        tableName=rrobin_name+repr(each)
        cursor.execute("Select * from " + tableName + " where Rating =  "+ repr(ratingValue))
        r = cursor.fetchall()
        for each in r:
            
            f.write(tableName + "," + repr(each[0]) + "," + repr(each[1]) + "," + repr(each[2]) + "\n")    
    f.close()
