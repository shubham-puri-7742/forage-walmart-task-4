import sqlite3
import csv
from pandas import *

'''
(FORMATS)
CSVs

shipping_data_0
   0         1          2        3        4       5
Origin, Destination, Product, OnTime, Quantity, Driver

shipping_data_1
0      1       2
ID, Product, OnTime

shipping_data_2
0      1         2         3
ID, Origin, Destination, Driver

TABLES

shipment
0       1          2        3         4
ID, ProductID, Quantity, Origin, Destination

product
0    1
ID, Name

'''

def setupDataMigration():
    db = sqlite3.connect('shipment_database.db')
    cur = db.cursor()
    print('Connected successfully.')
    
    cur.execute('''create table if not exists shipment_data (
                ID integer not null primary key,
                ProductID integer not null references product,
                Quantity integer not null,
                Origin text not null,
                Destination text not null)''')
    db.commit()
    cur.execute('''create table if not exists product_data (
                    ID integer not null primary key,
                    name text not null)''')
    db.commit()
    db.close()

def writeCsv0ToDb():
    try:
        print('Trying to read csv 0...')
        data = read_csv('data/shipping_data_0.csv')
        print('csv 0 read successfully')
    except:
        print('Cannot open csv 0')
        return

    db = sqlite3.connect('shipment_database.db')
    cur = db.cursor()
    print('Connected successfully.')

    # parsing the data
    origins = data['origin_warehouse'].tolist()
    dests = data['destination_store'].tolist()
    products = data['product'].tolist()
    qtys = data['product_quantity'].tolist()

    for i in range(len(origins)):
        cur.execute('insert into product_data values (?, ?)', (i, products[i]))
        db.commit()
        cur.execute('insert into shipment_data values (?, ?, ?, ?, ?)', (i, i, qtys[i], origins[i], dests[i]))
        db.commit()

    print('Stored csv 0 to the database. Closing...')
    db.close()

def writeCsv12ToDb():
    db = sqlite3.connect('shipment_database.db')
    cur = db.cursor()
    print('Connected successfully.')
    try:
        recs = {}
        print('Trying to read csv 1 and 2...')
        with open('data/shipping_data_1.csv') as f1:
            rd1 = csv.reader(f1)
            identifier = ''
            count = 0
            for r1 in rd1:
                if identifier == r1[0]:
                    count += 1
                else:
                    if count > 1:
                        with open('data/shipping_data_2.csv') as f2:
                            rd2 = csv.reader(f2)
                            i = -1
                            for r2 in rd2:
                                i += 1
                                if r2[0] == identifier:
                                    cur.execute('insert into product_data values (?, ?)', (i, r1[1]))
                                    db.commit()
                                    cur.execute('insert into shipment_data values (?, ?, ?, ?, ?)', (i, i, count, r2[1], r2[2]))
                                    db.commit()
                                    
        print('csv 1 and 2 read successfully')
    except:
        print('Cannot open csv 1 and 2')
        return
    print('Stored csv 1 and 2 to the database. Closing...')
    db.close()

if __name__ == '__main__':
    setupDataMigration()
    writeCsv0ToDb()
    writeCsv12ToDb()