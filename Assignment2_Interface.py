#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading
import logging
import time


# Do not close the connection inside this file i.e. do not perform openConnection.close()

def thread_function(pointPartition, rectanglePartition, cursor, index, spatialJoinResult):
    # This thread_function is called upon by each thread and performs the spatialjoin

    print("Thread will start here and perform the join operation")

    print(pointPartition)
    print(rectanglePartition)
    latitude = pointPartition + ".latitude"
    longitude = pointPartition + ".longitude"
    rectGeom = rectanglePartition + ".geom"
    pointGeom = pointPartition + ".geom"
    print(latitude, longitude, rectGeom, pointGeom)
    cursor.execute("Select %s,%s from %s,%s where ST_Contains(%s, %s)" % (latitude, longitude
                                                                          , pointPartition, rectanglePartition
                                                                          , rectGeom, pointGeom))

    joinResult = cursor.fetchall()
    spatialJoinResult.append(len(joinResult))
    print(len(joinResult), "Thread name is : ", index)


def parallelJoin(pointsTable, rectsTable, outputTable, outputPath, openConnection):
    # Implement ParallelJoin Here.
    print(openConnection)
    cur = openConnection.cursor()
    print(cur)
    print(type(pointsTable))

    #     Start creating the partitions
    pointPart1 = "pointpart1"
    pointPart2 = "pointpart2"
    pointPart3 = "pointpart3"
    pointPart4 = "pointpart4"

    pointPartList = [pointPart1, pointPart2, pointPart3, pointPart4]

    cur.execute("Create Table %s as Select * from %s where %s > 40.8" % (pointPart1, pointsTable, 'latitude'))

    cur.execute("Create Table %s as Select * from %s where %s > 40.7 and %s <=40.8" % (pointPart2, pointsTable,
                                                                                       'latitude', 'latitude'))

    cur.execute("Create Table %s as Select * from %s where %s > 40.6 and %s <=40.7" % (pointPart3, pointsTable,
                                                                                       'latitude', 'latitude'))

    cur.execute("Create Table %s as Select * from %s where %s <= 40.6" % (pointPart4, pointsTable,
                                                                          'latitude'))

    #     Start the space partitioning for rectangular object
    rectanglePart1 = "rectpart1"
    rectanglePart2 = "rectpart2"
    rectanglePart3 = "rectpart3"
    rectanglePart4 = "rectpart4"

    cur.execute("Create Table %s as Select * from %s where %s > 40.8" % (rectanglePart1, rectsTable, 'latitude1'))

    cur.execute("Create Table %s as Select * from %s where %s > 40.7 and %s <=40.8" % (rectanglePart2, rectsTable,
                                                                                       'latitude1', 'latitude1'))

    cur.execute("Create Table %s as Select * from %s where %s > 40.6 and %s <= 40.7" % (rectanglePart3, rectsTable,
                                                                                        'latitude1', 'latitude1'))

    cur.execute("Create Table %s as Select * from %s where %s <= 40.6" % (rectanglePart4, rectsTable, 'latitude1'))

    rectpartlist = [rectanglePart1, rectanglePart2, rectanglePart3, rectanglePart4]
    length_rect = len(rectpartlist)
    for index in range(length_rect):
        print(rectpartlist[index], index)

    threads = list()
    spatialJoinResult = list()

    # creating four threads to perform parallel join on each partition
    #   Thread 1 - SpatialJoin between pointpartition1 and rectanglePartition1
    #   Thread 2 - SpatialJoin between pointpartition2 and rectanglepartition2
    #   Thread 3 - SpatialJoin between pointpartition3 and rectanglePartition3
    #   Thread 4 - SpatialJoin between pointpartition4 and rectanglepartition4

    for index in range(length_rect):
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                            datefmt="%H:%M:%S")
        logging.info(" Create and start thread %d", index + 1)
        x = threading.Thread(target=thread_function,
                             args=(pointPartList[index], rectpartlist[index], cur, index + 1, spatialJoinResult))
        threads.append(x)
        x.start()
        time.sleep(10)

    for index, thread in enumerate(threads):
        logging.info("Main :before joining thread %d", index + 1)
        thread.join()
        logging.info(("Main : thread %d done", index + 1))

    # The counting of points in each partition join is stored and sorted in ascending order
    print("The spatial join result is : ")
    print(spatialJoinResult)
    print("The sorted list of a spatial join result")
    spatialJoinResult.sort()
    print(spatialJoinResult)

    # Printing the name of the Output table

    print(outputTable)

    # Now creating SQL commands to create a table called parallelJoinOutputTable

    sql = ''' CREATE TABLE parallelJoinOutputTable(
                                  partition_id INT,
                                   joinResult INT
                                   )'''
    cur.execute(sql)
    print("Table created successfully.........")
    index = 0;
    values = [(1, spatialJoinResult[index]), (2, spatialJoinResult[index+1]), (3, spatialJoinResult[index+2]),
              (4, spatialJoinResult[index+3])]
    cur.executemany("insert into parallelJoinOutputTable values(%s,%s)", values)
    sqlselect = ''' select * from parallelJoinOutputTable'''
    cur.execute(sqlselect)
    for i in cur.fetchall():
        print(i)

    # committing changes to a database
    openConnection.commit()
    # Writing to a file called output_part_a.txt

    print(outputPath)
    result_count0 = spatialJoinResult[0]
    result_count1 = spatialJoinResult[1]
    result_count2 = spatialJoinResult[2]
    result_count3 = spatialJoinResult[3]
    with open(outputPath, 'w') as f:
        f.write("No of points count for thread 1 is %s\n" %result_count1)
        f.write("No of points count for thread 2 is %s\n" %result_count3)
        f.write("No of points count for thread 3 is %s\n" %result_count2)
        f.write("No of points count for thread 4 is %s\n" %result_count0)

    print("Done writing to a file")

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# Donot change this function
def createDB(dbname='dds_assignment2'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()


# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
