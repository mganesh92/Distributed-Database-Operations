#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
from math import cos, sin, sqrt, atan2, radians
import json

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    cur=collection.find()
    file=open(saveLocation1,'w')
    for val in cur:
        if val['city'].lower()==cityToSearch.lower():
            file.write(val['name'].encode("utf-8").upper() + str('$') + val['full_address'].replace("\n", ", ").upper().encode('utf-8') +str('$') + val['city'].upper().encode('utf-8') + str('$') +val['state'].upper().encode('utf-8')+ str('\n'))

    pass

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    file=open(saveLocation2,'w')
    cur=collection.find()
    length=len(categoriesToSearch)
    for val in cur:
        if distance(val['latitude'], val['longitude'], float(myLocation[0]), float(myLocation[1])) < maxDistance:
            count=0
            for category in categoriesToSearch:
                if category.lower() in [x.lower() for x in val['categories']]:
                    count=count+1
                    if count==length:
                        file.write(val['name'].encode("utf-8").upper() + str('.\n'))

    pass

def distance(lat2, lon2, lat1, lon1):
    R = 3959
    radian1 = radians(lat1)
    radian2 = radians(lat2)
    theta = radians(lat2-lat1)
    lamda = radians(lon2 - lon1)
    a = sin(theta/2)*sin(theta/2) + cos(radian1)*cos(radian2)*sin(lamda/2)*sin(lamda/2)
    c = 2*atan2(sqrt(a), sqrt(1-a))
    d = R*c
    return d
