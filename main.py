__author__ = 'Jiecheng (Jason) Xu'
__email__ = 'xjcpdp1995@gmail.com'
__version__ = '1.0'

import pymysql as sq
import requests
import pandas as pd
import time
import sys
import copy

# download files from iso-ne.com based on user selections
def download_file(date, year, ms, me):
    result_list = []
    url = 'http://iso-ne.com/static-transform/csv/histRpts/da-lmp/WW_DALMP_ISO_{}.csv'.format(date)
    try:
        LMR = requests.get(url)
        lmr_name = date + '.csv'
        with open(lmr_name, 'wb') as file:
            file.write(LMR.content)
        print('Successfully Download Day-Ahead Market Information!')
    except:
        print('Oops, something went wrong.')
    # download short term results csvs
    for m in range(int(ms), int(me) + 1):
        url = "http://iso-ne.com/transform/csv/ftrauctionresults?type=monthly&month={}".format(
            year + str(m).rjust(2, '0'))
        try:
            result_csv = requests.get(url)
            file_name = 'shortterm' + year + str(m).rjust(2, '0') + '.csv'
            with open(file_name, 'wb') as file:
                file.write(result_csv.content)
            print('Successfully Download Short Term Auction Result!')
            result_list.append(file_name)
        except:
            print('Oops, something went wrong')

    # download long term results csvs
    for i in [1, 2]:
        url = 'http://iso-ne.com/transform/csv/ftrauctionresults?type=long_term_{}&year={}'.format(i, year)
        try:
            long_term = requests.get(url)
            file_name = 'longterm' + str(i) + year + '.csv'
            with open(file_name, 'wb') as file:
                file.write(long_term.content)
            print('Successfully Download Long Term Auction Result!')
            result_list.append(file_name)
        except:
            print('Oops, something went wrong')
    return result_list, lmr_name


# insert auction id and name pair into auction table
# auction_name: store all unique auction names
# auction_id: self-incrementing auction id
def populate_auction_table(fl: pd.DataFrame, db: sq.connect):
    global auc_name2ID, auction_id
    with db.cursor() as cursor:
        auction_name = fl['Auction Name'].unique()
        for name in auction_name:
            period = 365
            if len(name.split(' ')) == 3:
                period = 30
            try:
                sql = 'INSERT INTO `auction` (Auc_ID, Auc_Name, Period) ' \
                      'VALUES (%s, \'%s\', %s)' % (auction_id, name, period)
                auc_name2ID[name] = auction_id
                cursor.execute(sql)
                auction_id += 1
                db.commit()
            except:
                db.rollback()


# insert customer id and customer name into customer table
# both c_id and c_name are parsed from the data frame
def populate_customer_table(fl: pd.DataFrame, db: sq.connect):
    with db.cursor() as cursor:
        customer = fl[['Customer ID', 'Customer Name']].drop_duplicates()
        for row in customer.itertuples(index=False):
            c_id = row[0]
            c_name = row[1]
            try:
                sql = 'INSERT INTO `customer` (C_ID, C_Name) VALUES (%s, \'%s\')' % (c_id, c_name)
                cursor.execute(sql)
                # print('1 row inserted')
                db.commit()
            except:
                # print('Failed to insert into Customer table, PK already exists.')
                db.rollback()


# insert location id, location name, location type into location table
def populate_location_table(fl: pd.DataFrame, db: sq.connect):
    global lmr
    with db.cursor() as cursor:
        lmr = pd.read_csv(lmr, skiprows=4)
        lmr.drop(lmr.tail(1).index, inplace=True)
        lmr.drop(0, inplace=True)

        # insert location id, location name, location type into location table
        # l_id, l_name and l_type are parsed from the data frame
        location = lmr[['Location ID', 'Location Name', 'Location Type']].drop_duplicates()
        for row in location.itertuples(index=False):
            l_id = row[0]
            l_name = row[1]
            l_type = row[2]
            try:
                sql = 'INSERT INTO `location` (L_ID, L_Name, L_Type) VALUES (%s, \'%s\', \'%s\')' % (
                    l_id, l_name, l_type)
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()
                
        # Insert location information again parsed from auction result data frame, this is for the case that something
        # appeared in the result table was not in the LMR table, doing this could help cover these missing locations
        location = fl[['Source Location ID', 'Source Location Name', 'Source Location Type']].drop_duplicates()
        for row in location.itertuples(index=False):
            l_id = row[0]
            l_name = row[1]
            l_type = row[2]
            try:
                sql = 'INSERT INTO `location` (L_ID, L_Name, L_Type) VALUES (%s, \'%s\', \'%s\')' % (
                    l_id, l_name, l_type)
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()

        # do the same thing for sink location
        location = fl[['Sink Location ID', 'Sink Location Name', 'Sink Location Type']].drop_duplicates()
        for row in location.itertuples(index=False):
            l_id = row[0]
            l_name = row[1]
            l_type = row[2]
            try:
                sql = 'INSERT INTO `location` (L_ID, L_Name, L_Type) VALUES (%s, \'%s\', \'%s\')' % (
                    l_id, l_name, l_type)
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()


# Populate the main auction_result table, table contains Auc_ID, C_ID, Source_ID, Sink_ID, Buy_Sell, Class_Type,
# Award_FTP_MW, Award_FTP_Price, this table will be mainly used to access different information
def populate_aucresult_table(fl: pd.DataFrame, db: sq.connect):
    global auc_name2ID, result_id
    with db.cursor() as cursor:
        auc_result = fl[['Auction Name', 'Customer ID', 'Source Location ID', 'Sink Location ID', 'Buy/Sell',
                         'ClassType', 'Award FTR MW', 'Award FTR Price']]
        for row in auc_result.itertuples(index=False):
            auc_id = auc_name2ID[row[0]]
            c_id = row[1]
            source_id = row[2]
            sink_id = row[3]
            b_s = row[4]
            class_type = row[5]
            ftr_mw = float(row[6])
            ftr_price = float(row[7])
            try:
                sql = 'INSERT INTO `auction_results` (Auc_Res_ID, Auc_ID, C_ID, Sou_ID, Sin_ID, Buy_Sell, ' \
                      'Class_Type, Award_FTR_MW, Award_FTR_Price ) VALUES (%s, %s, %s, %s, %s, \'%s\',' \
                      ' \'%s\', %s, %s)' % (
                          result_id, auc_id, c_id, source_id, sink_id, b_s, class_type, ftr_mw, ftr_price)
                result_id += 1
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()


# Populate the day ahead market table, table contains L_ID, Hour_Ending, Date, LMP, Energy_Comp, Cog_Comp, ML_Comp
# This table will be mainly used to refer price information during the calculation
def populate_lmp_table(db: sq.connect):
    global lmr
    # print(lmr)
    # lmr = pd.read_csv(lmr, skiprows=4)
    # lmr.drop(lmr.tail(1).index, inplace=True)
    # lmr.drop(0, inplace=True)
    with db.cursor() as cursor:
        data = lmr[['Location ID', 'Hour Ending', 'Date', 'Locational Marginal Price', 'Energy Component',
                    'Congestion Component', 'Marginal Loss Component']]
        for row in data.itertuples(index=False):
            l_id = row[0]
            hour_ending = row[1]
            m, d, y = row[2].split('/')
            date = y + '-' + m + '-' + d
            lmp = row[3]
            ene_comp = row[4]
            cog_comp = row[5]
            ml_comp = row[6]
            try:
                sql = 'INSERT INTO `day_ahead_market` (L_ID, Hour_Ending, Date, LMP, Energy_Comp, Cog_Comp, ML_Comp) ' \
                      'VALUES (%s, %s, \'%s\', %s, %s, %s, %s)' % (
                          l_id, hour_ending, date, lmp, ene_comp, cog_comp, ml_comp)
                cursor.execute(sql)
                db.commit()
            except:
                # print(sql)
                db.rollback()


# read all auction result csv in to data frame and populate the database
def populate_database(result_list: list):
    global auction_id, auc_name2ID, lmr, host, username, pswd
    db = sq.connect(host=host, user=username, password=pswd, db='proj')
    df_list = []
    # iterate through the list, open every file in a data frame and concat to create a data frame contains all rows
    for file in result_list:
        df = pd.read_csv(file, skiprows=4)
        df.drop(df.tail(1).index, inplace=True)
        df.drop(0, inplace=True)
        df_list.append(df)
    fl = pd.concat(df_list)

    pd.set_option('display.max_columns', 100)
    pd.set_option('display.width', 1000)

    # initialize all database tables and perform data insertion
    print('')
    print('Please be aware, this initializing process might take 5 - 8 minutes')
    print('')
    print('Initializing Auction...')
    populate_auction_table(fl, db)

    print('Initializing Customer...')
    populate_customer_table(fl, db)

    print('Initializing Location...')
    populate_location_table(fl, db)

    print('Initializing Auction Results...')
    populate_aucresult_table(fl, db)

    print('Initializing Day Ahead Market...')
    populate_lmp_table(db)

    print('Setting up foreign keys...')
    set_fk(db)

    print('Done! Database Initialized.')
    db.close()

# Based on the user input customer name, calculate that customer's total profit based on the day ahead market
# Buy count for a coefficient of -1 on the revenue and Sell count for a coefficient of 1 on the revenue
def calculate_customer_profit(customer_name: str) -> float:
    global host, username, pswd, y, m
    db = sq.connect(host=host, user=username, password=pswd, db='proj')
    try:
        c_id = get_c_id(customer_name, db)[0]
    except Exception as e:
        print('Customer Name not in database, Please try again.')
        sys.exit(0)
    with db.cursor() as cursor:
        # tried to use the commented out sql query to extract all data at once but encountered a unsolvable bug that will
        # cause the program to hang forever when user choose to initialize data base
        # the query itself works fine if user choose not to create a new database
        #
        # sql = """SELECT C_ID, hr, Sou_ID, c1, Sin_ID, Cog_Comp AS c2, \
        #       CASE WHEN Buy_Sell = 'BUY' THEN -1 \
        #       WHEN Buy_Sell = 'SELL' THEN 1 END AS BS , \
        #       CASE WHEN Class_Type = 'OFFPEAK' THEN 1/8 \
        #       WHEN Class_Type = 'ONPEAK' THEN 1/16 END AS Class_Type, \
        #       Award_FTR_MW, Award_FTR_Price, Period \
        #       FROM ( SELECT C_ID,Hour_Ending AS hr, Sou_ID, Cog_Comp AS c1, Sin_ID, Buy_Sell, Class_Type, \
        #       Award_FTR_MW, Award_FTR_Price, Period FROM auction_results \
        #       JOIN auction ON auction_results.Auc_ID = auction.Auc_ID \
        #       JOIN day_ahead_market ON (auction_results.Sou_ID = day_ahead_market.L_ID) \
        #       WHERE C_ID = {} AND (Auc_Name LIKE '%{} {}' OR Auc_Name LIKE '%{}')) AS X \
        #       JOIN day_ahead_market ON (X.Sin_ID = day_ahead_market.L_ID \
        #       AND hr = day_ahead_market.Hour_Ending) \
        #       WHERE (X.Class_Type = 'OFFPEAK' \
        #       AND hr IN (24,1,2,3,4,5,6,7)) \
        #       OR (X.Class_Type = 'ONPEAK' AND hr IN (8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23));""".format (c_id, y,m,y)

        query = """SELECT C_ID,Hour_Ending, Sou_ID, Cog_Comp AS c1, CASE WHEN Buy_Sell = 'BUY' THEN 1 \
        WHEN Buy_Sell = 'SELL' THEN -1 END AS BS, Award_FTR_MW, Award_FTR_Price, Period \
        FROM auction_results JOIN auction ON auction_results.Auc_ID = auction.Auc_ID \
        JOIN day_ahead_market ON auction_results.Sou_ID = day_ahead_market.L_ID \
        WHERE C_ID = {} AND (Auc_Name LIKE '%{} {}' OR Auc_Name LIKE '%{}') \
        AND ((Class_Type = 'OFFPEAK' AND Hour_Ending IN (24,1,2,3,4,5,6,7)) \
        OR (Class_Type = 'ONPEAK' AND Hour_Ending IN (8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)));""".format(c_id, y, m, y)
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        df.columns = [i[0] for i in cursor.description]
        sou_cc = df['c1']
        b_s = df['BS']
        ftr_mw = df['Award_FTR_MW']
        ftr_price = df['Award_FTR_Price']
        period = df['Period']

        query = """SELECT Sin_ID, CASE WHEN Class_Type = 'OFFPEAK' THEN 1/8 \
        WHEN Class_Type = 'ONPEAK' THEN 1/16 END AS Class_Type, Cog_Comp AS c2 \
        FROM auction_results JOIN day_ahead_market ON auction_results.Sin_ID = day_ahead_market.L_ID \
        JOIN auction ON auction_results.Auc_ID = auction.Auc_ID \
        WHERE C_ID = {} AND (Auc_Name LIKE '%{} {}' OR Auc_Name LIKE '%{}') \
        AND ((Class_Type = 'OFFPEAK' AND Hour_Ending IN (24,1,2,3,4,5,6,7)) \
        OR (Class_Type = 'ONPEAK' AND Hour_Ending IN (8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23)));""".format(c_id, y, m, y)
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result)
        df.columns = [i[0] for i in cursor.description]
        on_off = df['Class_Type']
        sin_cc = df['c2']
        profit = b_s * (((sou_cc - sin_cc) * ftr_mw ) - ((ftr_mw * ftr_price * on_off) / period))
        return sum(profit)



# Take in a customer name, and return the corresponding customer id for future reference
def get_c_id(customer_name: str, db: sq.connect):
    with db.cursor() as cursor:
        sql = 'SELECT * FROM customer WHERE C_Name = \'%s\';' % customer_name
        cursor.execute(sql)
        return cursor.fetchone()


# A switcher function, take in a int and return the corresponding month string
def get_month_to_str(m: str):
    switch_m = {
        1: 'JAN',
        2: 'FEB',
        3: 'MAR',
        4: 'APR',
        5: 'MAY',
        6: 'JUN',
        7: 'JUL',
        8: 'AUG',
        9: 'SEP',
        10: 'OCT',
        11: 'NOV',
        12: 'DEC'
    }
    return switch_m[int(m)]


# Set up FKs in the database, some of the FKs might fail because we can not guarantee data integrity based on one single
# day ahead market, some of the location might not show up in the day ahead market table and therefore, location FK
# might fail
def set_fk(db: sq.connect):
    with db.cursor() as cursor:
        # populate fk in auction result tables
        try:
            sql = 'ALTER TABLE `proj`.`auction_results` ' \
                  'ADD CONSTRAINT `auction_fk` FOREIGN KEY (`Auc_ID`) REFERENCES `proj`.`auction` (`Auc_ID`) ' \
                  'ON DELETE NO ACTION ON UPDATE NO ACTION;'
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
        try:
            sql = 'ALTER TABLE `proj`.`auction_results` ' \
                  'ADD CONSTRAINT `source_fk` FOREIGN KEY (`Sou_ID`) REFERENCES `proj`.`location` (`L_ID`) ' \
                  'ON DELETE NO ACTION ON UPDATE NO ACTION;'
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
        try:
            sql = 'ALTER TABLE `proj`.`auction_results` ' \
                  'ADD CONSTRAINT `sink_fk` FOREIGN KEY (`Sin_ID`) REFERENCES `proj`.`location` (`L_ID`) ' \
                  'ON DELETE NO ACTION ON UPDATE NO ACTION;'
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
        try:
            sql = 'ALTER TABLE `proj`.`auction_results` ' \
                  'ADD CONSTRAINT `sou_lmp_fk` FOREIGN KEY (`Sou_ID`) REFERENCES `proj`.`day_ahead_market` (`L_ID`) ' \
                  'ON DELETE NO ACTION ON UPDATE NO ACTION;'
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
        try:
            sql = 'ALTER TABLE `proj`.`auction_results` ' \
                  'ADD CONSTRAINT `sin_lmp_fk` FOREIGN KEY (`Sin_ID`) REFERENCES `proj`.`day_ahead_market` (`L_ID`) ' \
                  'ON DELETE NO ACTION ON UPDATE NO ACTION;'
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()
        try:
            sql = 'ALTER TABLE `proj`.`day_ahead_market` ADD CONSTRAINT `location_fk` FOREIGN KEY (`L_ID`) ' \
                  'REFERENCES `proj`.`location` (`L_ID`) ' \
                  'ON DELETE NO ACTION ON UPDATE NO ACTION;'
            cursor.execute(sql)
            db.commit()
        except:
            db.rollback()


# Create a new database schema using existing sql script
def create_database():
    global username, host, pswd
    sql = read_Sql_File('mysql_script.sql')
    with sq.connect(host=host, user=username, password=pswd) as db:
        for q in sql:
            try:
                db.execute(q)
            except Exception as e:
                print(str(e))

# This is a parser that take in a .sql script file path and return a list of separate sql statements
def read_Sql_File(path):
    f = open(path, "r", encoding="UTF-8")
    lines = f.readlines()

    sqlList = []
    thisSql = ""
    mulNote = False
    for line in lines:
        string = str(line).strip()
        if string == "":
            continue

        # part1 multi-line comment
        if mulNote:
            if string.startswith("*/"):
                mulNote = False
            continue
        if string.startswith("/*"):
            mulNote = True
            continue
        if string.startswith("#") or string.startswith("--"):
            continue

        strIn1 = False
        strIn2 = False
        for i in range(len(string)):
            c = string[i]
            # part2 string in sql
            if "'" == c:
                if not strIn1:
                    strIn1 = True
                else:
                    strIn1 = False
                continue

            if '"' == c:
                if not strIn2:
                    strIn2 = True
                else:
                    strIn2 = False
                continue

            if strIn1 is True and strIn2:
                continue

            # part3 end of sql
            if ";" == c:
                string = string[0:(i + 1)]
                break

            # part4 comment behind of the sql
            if "#" == c:
                string = string[0:i]
                break
            if "-" == c and i <= len(string) - 2 \
                    and "-" == string[i + 1]:
                string = string[0:i]
                break

        # part5 join multi-line for one sql
        thisSql += " " + string

        # part6 end of sql
        if string.endswith(";"):
            sqlList.append(copy.deepcopy(thisSql))
            thisSql = ""

    return sqlList


def populate_profit(profit: float):
    global host, username, pswd, customer_name, mon, y, d
    db = sq.connect(host=host, user=username, password=pswd, db='proj')
    with db.cursor() as cursor:
        sql = 'INSERT INTO `proj`.`profit` (C_ID, C_Name, Date_Of_Profit, Profit) ' \
              'VALUES (%s, \'%s\', \'%s\', %s)' % (
                  get_c_id(customer_name, db)[0], customer_name, y + '-' + mon + '-' + d, profit)
        try:
            cursor.execute(sql)
            print('1 row inserted')
            db.commit()
        except Exception as e:
            print('Failed to insert into Profit table')
            print(str(e))
            db.rollback()
    db.close()

# Main UI starts here
if __name__ == '__main__':
    # Get user inputs, initialize customer_name and date
    print('Please enter your database host: ')
    host = input()
    print('Please enter your database username: ')
    username = input()
    print('Please enter your database password: ')
    pswd = input()
    print('')

    print(
        'Would you like to create a new database? '
        'Note: This will create a new database and wipe out the previous data. (Y/N):')
    c = input()
    if c == 'Y':
        print('Creating MySQL database...')
        create_database()
        print('')

    print('Please Enter a customer name: ')
    customer_name = input()

    print('Please Enter a year you are looking for: ')
    year = input()
    print('Please enter a range of month you are looking for using format MM-MM: ')
    ms, me = input().split('-')

    print('Please Enter a date for day-ahead market using format MM/DD/YYYY: ')
    mon, d, y = input().split('/')
    date = y + mon + d

    # # These are variables stores all downloaded files
    result_list, lmr = download_file(date, year, ms, me)

    # global variables defined down below
    auction_id = 1  # this is a self-incrementing variable to record ids for different auction names
    auc_name2ID = {}  # this is a global variable used to store pairs of auction name and its unique generated ids
    result_id = 1  # this is a self-incrementing variable to record result ids in the main auction_result table

    m = get_month_to_str(mon)

    # populate data base
    s_time = time.time()
    populate_database(result_list)
    print('')
    print('The initializing process took: %1.2f minutes' % ((time.time() - s_time) / 60))
    print('')

    print('Calculating Profit...')
    profit = round(calculate_customer_profit(customer_name), 2)
    print("The customer's profit on that day is: %6.2f USD" % (profit))
    print('Inserting into profit table...')
    populate_profit(profit)
    print('Done!')
