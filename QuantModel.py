# IMPORT BASE MODULES:
import os
import sys
import datetime
import subprocess
import string
import time
import traceback
import subprocess
import socket
import optparse
import MySQLdb

#define variables for directory/file names
this=os.path.basename(sys.argv[0])

#home_dir           -- home dir
#data_export_dir    -- data retrieved from tdx exportation
#data_ipsrc_dir     -- source data copied from export dir (the same, so far)
#data_opsrc_dir     -- source data processed (truncated the last line) based on ipsrc_data
#data_procd_dir     -- data processed based on opsrc_data
home_dir = "D:\\Personal_Misc\\0000_Investment\\QuantitativeModel\\"
data_export_dir = home_dir + "export"
data_ipsrc_dir = home_dir + "ipsrcdata"
data_opsrc_dir = home_dir + "opsrcdata"
data_procd_dir = home_dir + "procddata"
code_file = 'SH000001.data'

###############################################################################
# FUNCs:
###############################################################################
            
###############################################################################
def tryMySQL():
    #This is just a test for MySQL db operations
    
    try:
        # command is like 'show databases'
        conn=MySQLdb.connect(host='localhost',user='root',passwd='xushang1984',db='test',port=3306)
        cur=conn.cursor()
        cur.execute('select * from user')
        values = cur.fetchall()
        cur.close()
        conn.close()

        print(values)

    except MySQLdb.Error, exc:
        print(traceback.format_exc())
	raise Exception, 'tryMySQL() failed'
     
###############################################################################
#import data from disk file into MySQL database
def importData():
    
    try:
        #Before following steps, need to create database and tables, manually create them temporally
        #database: quantmodel
        #table: sh000001
        #create table sh000001 ( ind int NOT NULL, date char(30), opening char(30), highest char(30), lowest char(30), closing char(30), volume char(30), turn_volume char(30), primary key (index)) engine=InnoDB;

        #read data from disk file into list
        workload = 'SH000001.data'

        #MySQL db connection
        conn=MySQLdb.connect(host='localhost',user='root',passwd='xushang1984',db='quantmodel',port=3306)
        cur=conn.cursor()

        indx = 0
        for line in open(workload, 'r'):
            # workload list
            wl = []
        
            if line[0:1] == '#': 
                #comments
                continue

            #get workload lines as wl list
            llist = line.strip().split()
            for i in llist:
                if i != '':
                    wl.append(i)
                
            #date opening highest lowest closing volume turn_volume
            date        =   wl[0]
            opening     =   wl[1]
            highest     =   wl[2]
            lowest      =   wl[3]
            closing     =   wl[4]
            volume      =   wl[5]
            turn_volume =   wl[6]

            cur.execute('insert into sh000001(ind, date, opening, highest, lowest, closing, volume, turn_volume) values (%s, %s, %s, %s, %s, %s, %s, %s)', [ str(indx), date, opening, highest, lowest, closing, volume, turn_volume])         

            indx = indx + 1

        conn.commit()    
        cur.close()
        conn.close()

    except MySQLdb.Error, exc:
        print(traceback.format_exc())
	raise Exception, 'importData() failed'

###############################################################################
def calc_MA(closing_list, index, length):

    #ma value, float type. If not appliable, just return 0
    ma_value = 0.0

    #return 0.0 if not applicable
    if index < length-1:
        return ma_value
    
    #list for closing quatation
    cl = closing_list

    i = 0
    while ( i < length ):
        ma_value = ma_value + float(cl[index-i])
        i = i + 1

    ma_value = ma_value/length

    ma_value = ("%.2f" %ma_value) #save 2 bit after .

    return float(ma_value)

###############################################################################
#This funcs is to process source data to remove unusable lines
#source data format:
#header
#data
#ender - chinese, need to remove this line
##data_export_dir = home_dir + "export"
##data_ipsrc_dir = home_dir + "ipsrcdata"
##data_opsrc_dir = home_dir + "opsrcdata"
def procSrcData():
    
    try:
        ipfList = os.listdir(data_ipsrc_dir)

        for tmpfn in ipfList:
            ipf = data_ipsrc_dir + "\\" + tmpfn
            fd = open(ipf, 'r')
            aList = fd.readlines()
            #pop up the last line
            aList.pop()
            fd.close()

            opf = data_opsrc_dir + "\\" + tmpfn.split('.')[0]
            opf = opf + '.data'
            fd = open(opf, 'w')
            fd.writelines(aList)
            fd.close()

    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'procSrcData() failed'

###############################################################################
#This funcs is to import all the data in opsrcdata dir into database
def importDataAll_MAs():
    
    try:
        opfList = os.listdir(data_opsrc_dir)

        for tmpfn in opfList:
            filename = data_opsrc_dir + "\\" + tmpfn
            tablename = tmpfn.split('.')[0]
            dbname = 'quantmodel'
        
            importData_MAs(filename, dbname, tablename)

    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'importDataAll_MAs() failed'
    
###############################################################################
#import data from disk file into MySQL database, then calcalute MAs (5,10,20,30,60,120,250)
def importData_MAs(filename, dbname, tablename):
    
    try:
        #Before following steps, need to create database and tables
        #Steps:
        #1). use database
        #2). drop table if exists
        #3). read data from diskfile and import into database
        #4). calc MAs
        #database: quantmodel
        #table: sh000001
        #schema: indx, date, opening, hightest, lowest, closing, volume, amount, ma5, ma10, ma20, ma30, ma60, ma120, ma250
        #create table sh000001 ( indx int NOT NULL, date char(30), opening char(30), highest char(30), lowest char(30), closing char(30), volume char(30), turn_volume char(30), primary key (index)) engine=InnoDB;

        #read data from disk file into list
        #data_dir = "D:\\Personal_Misc\\0000_Investment\\QuantitativeModel\\data\\"
        workdata = filename
        database = dbname
        table = tablename

        #MAs
        ma5 = 0
        ma10 = 0
        ma20 = 0
        ma30 = 0
        ma60 = 0
        ma120 = 0
        ma250 = 0

        #table tuple lists
        date_list       = []
        opening_list    = []
        highest_list    = []
        lowest_list     = []              
        closing_list    = []
        volume_list     = []
        amount_list     = []
        
        #MySQL db connection
        conn=MySQLdb.connect(host='localhost',user='root',passwd='xushang1984',db=database,port=3306)
        cur=conn.cursor()

        #drop table before import data from disk file
        sqlcmd = 'drop table if exists ' + table
        cur.execute(sqlcmd)

        #create table
        print ('------------------------------------')
        print('Now we start to create table ' + table + ' in database ' + database )
        sqlcmd = 'create table ' + table + '( indx int NOT NULL, date char(30), opening char(30), highest char(30), '\
                                             'lowest char(30), closing char(30), volume char(30), amount char(30), '\
                                             'ma5 float, ma10 float, ma20 float, ma30 float, ma60 float, '\
                                             'ma120 float, ma250 float, primary key(indx) ) engine=InnoDB;'
        cur.execute(sqlcmd)
        print('Now we finish table creation')

        print('Now we start to read data from disk file and import into database')
        indx = 0
        for line in open(workdata, 'r'):
            # workdata list
            wd = []
        
            if line[0:1] == '#': 
                #comments
                continue

            #get workdata lines as wd list
            llist = line.strip().split()
            for i in llist:
                if i != '':
                    wd.append(i)
                
            #date opening highest lowest closing volume amount
            date        =   wd[0]
            opening     =   wd[1]
            highest     =   wd[2]
            lowest      =   wd[3]
            closing     =   wd[4]
            volume      =   wd[5]
            amount      =   wd[6]

            #append closing list for calculating MAs
            closing_list.append(closing)

            #Now calc MAs if applies
            ma5   = calc_MA(closing_list, indx, 5)
            ma10  = calc_MA(closing_list, indx, 10)
            ma20  = calc_MA(closing_list, indx, 20)
            ma30  = calc_MA(closing_list, indx, 30)
            ma60  = calc_MA(closing_list, indx, 60)
            ma120 = calc_MA(closing_list, indx, 120)
            ma250 = calc_MA(closing_list, indx, 250)

            #insert tuple into table
            sqlcmd = 'insert into ' + tablename + ' ( indx, date, opening, highest, lowest, closing, volume, amount, '\
                     'ma5, ma10, ma20, ma30, ma60, ma120, ma250) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
            cur.execute(sqlcmd, (indx, date, opening, highest, lowest, closing, volume, amount, ma5, ma10, ma20, ma30, ma60, ma120, ma250) )         

            indx = indx + 1

        print('Now we complete data importion for table ' + tablename)
        print ('------------------------------------')
        
        conn.commit()    
        cur.close()
        conn.close()

    except MySQLdb.Error, exc:
        print(traceback.format_exc())
	raise Exception, 'importData_MAs() failed'

###############################################################################
#This func is to calculate the profit points buying and selling using MA signal
#This method is too much DB exausted. 
def transact_MAs_fromDB(dbname, tablename, malength):
    
    try:
        
        database = dbname
        table = tablename

        #table tuple list for closing           
        closing_list    = []
  
        #MySQL db connection
        conn=MySQLdb.connect(host='localhost',user='root',passwd='xushang1984',db=database,port=3306)
        cur=conn.cursor()

        #check if table has enough tuples
        sqlcmd = "select count(*) from " + table
        cur.execute(sqlcmd)
        linenum = cur.fetchone()
        if linenum < malength:
            print('No enough tuple in ' + tablename + ' to execute MA calculation.')
            return 

        #start to interate the whole table from indx (malength -1)
        indx = malength - 1
        while indx < linenum:
            sqlcmd = 'select closing from ' + table + ' where indx = ' + indx;
            cur.execute(sqlcmd)
            closing = cur.fetchone()

            #Need to calc MA value here
            mavalue   = calc_MA(closing_list, indx, malength)

            indx = indx + 1

        conn.commit()    
        cur.close()
        conn.close()        
        
    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'profit_MAs() failed'

###############################################################################
#This func is to calculate the profit points buying and selling using MA signal
#This method is using data from disk file directly.  
def transact_MAs_fromDF(codename, malength):
    
    try:

        #check the line number, if linenum < malength, then just return
        workdata = data_opsrc_dir + "\\" + codename + '.data'
        fd = open(workdata, 'r')
        aList = fd.readlines()
        linenum = len(aList)
        fd.close()
        if linenum < malength:
            print('No enough line records to execute MA calculation.')
            return 0

        #table tuple list for closing           
        closing_list    = []

        #list for buyin point and sellout point
        #format: [date, closing]
        buyin_point     = []
        sellout_point   = []
        earned_points   = [] # earnedpoint = selloutpoint - buyinpoint
        buyin_sellout_earned_list = [] # format: [[], [], []]
        #total profit during the interval
        #tp[0] - total absolute value
        #tp[1] - total percent 
        total_profit = []
        #summary list
        summaryList = []

##        #operation action: buyin or sellout or onhold (no operation)
##        buyin = False
##        sellout = False
##        onhold = True
##        #position status: longposition or shortposition
##        longpostn = False
##        shortpostn = True
        #InAll
        #status: 0 (shortpostn); 1 (longpostn)
        #action: 0 (onhold), 1 (buying); 2 (sellout)
        status = 0
        action = 0

        #print ('------------------------------------')
        
        indx = 0
        for line in open(workdata, 'r'):
            
            # workdata list
            wd = []
            
            #get workdata lines as wd list
            llist = line.strip().split()
            for i in llist:
                if i != '':
                    wd.append(i)
                          
            #date opening highest lowest closing volume amount  
            date        =   wd[0]
            opening     =   wd[1]
            highest     =   wd[2]
            lowest      =   wd[3]
            closing     =   float(wd[4])
            volume      =   wd[5]
            amount      =   wd[6]

            #append closing list for calculating MAs
            closing_list.append(closing)

            if indx < malength -1:
                #there is no ma value so far
                indx = indx + 1
                continue

            #Now calc MA of malength
            mavalue   = calc_MA(closing_list, indx, malength)

            #main algorithm
            #status -> position status
            #action -> position action
            #status:    longpostn   shortpostn
            #action:    buyin       sellout     onhold
            #InAll
            #status:
            #   0 (shortpostn or empty position)
            #   1 (longpostn or full position)
            #action:
            #   0 (onhold)  -> status no change
            #   1 (buying)  -> status = 1
            #   2 (sellout) -> status = 0
            #Initial value:
            #status = 0
            #action = 0
            #One thought on sellout and buyin point: use the mavalue -- we sell or buy once it cross the mavalue
            if closing >= mavalue:
                
                #either buyin or onhold
                if status == 0:
                    #need to buyin and record the date and closing value at this buyin time
                    action = 1
                    status = 1
                    buyin_point = [date, closing, mavalue]
                    
                elif status == 1: #full position already
                    #need to just onhold, status no change
                    action = 0
                    status = 1

            elif closing < mavalue:
                
                #either sellout or onhold
                if status == 0:
                    # need to onhold, status no change
                    action = 0
                    status = 0

                elif status == 1:
                    # need to sellout, then calc the earned points (maybe positive or negetive)
                    action = 2
                    status = 0
                    sellout_point   = [date, closing, mavalue]
                    earnedpoints    = sellout_point[1] - buyin_point[1]
                    earned_points   = [date, earnedpoints]
                    buyin_sellout_earned_list.append([buyin_point, sellout_point, earned_points])
         
            indx = indx + 1

        #after the main algorithm, here we come to the last value
        #need to check if the last value is larger than mavalue
        #Here if status == 1, means there is only buyin_point and there is no sellout_point
        #So we use the last closing value as sellout_point
        #Of course there is chance that sellout date is same as buyin date...
        #For the profit of the last value, 3 solutions:
        #   1) selloutpoint - buyinpoint
        #   2) mavalue - buyinpoint
        #   3)( mavalue + (selloutpoint - mavalue)/2 ) - buyinpoint
        if status == 1:
            sellout_point = [date, closing, mavalue]
            #earnedpoints    = sellout_point[1] - buyin_point[1]
            #earnedpoints    = mavalue - buyin_point[1]
            earnedpoints    = (mavalue + sellout_point[1])/2 - buyin_point[1]
            earned_points   = [date, earnedpoints]
            buyin_sellout_earned_list.append([buyin_point, sellout_point, earned_points])                     

        #SUMMARY
        #buyin date; buyin point; sellout date; sellout point; profit; profit percent
        #calc total profit and total profit percent at the same time; both are float number
        total_profit_point = 0.0
        total_profit_percent = 0.0
        outfile = data_procd_dir + "\\" + codename + ".out"
        fd = open(outfile, 'a')
        fd.write('The data for MA'+str(malength)+':\n')
        #fd.write('buyin date;   buyin point;    buyin mavalue;      sellout date;    sellout point;     sellout mavalue;     profit;     profit percent\n')
        for bse in buyin_sellout_earned_list:
            buyin_date      = bse[0][0]
            buyin_point     = bse[0][1]
            buyin_mavalue   = bse[0][2]
            sellout_date    = bse[1][0]
            sellout_point   = bse[1][1]
            sellout_mavalue = bse[1][2]
            profit_point    = bse[2][1]
            profit_percent  = profit_point/buyin_point
            total_profit_point   = total_profit_point + profit_point
            #total_profit_percent = total_profit_percent + profit_percent
            total_profit_percent = total_profit_percent + profit_percent + total_profit_percent*profit_percent
            fd.write(buyin_date + ';    \t' + str(buyin_point) + ';     \t' + str(buyin_mavalue) + ';   \t' + sellout_date + ';     \t' + str(sellout_point) + ';   \t' + str(sellout_mavalue) + ';     \t' + str(profit_point) + ';    \t' + str(profit_percent) + '\n')
        fd.close()
        
        #profit SUMMARY
##        for bse in buyin_sellout_earned_list:
##            total_profit = total_profit + bse[2][1]
##        print('Total profit by MA' + str(malength) + ' is ' + str(total_profit))
        #print('Total profit by MA' + str(malength) + ' is ' + str(total_profit_point) + '\t' + str(total_profit_percent))

        #return summaryList
        total_profit = [total_profit_point, total_profit_percent]
        return total_profit
        
    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'transact_MAs_fromDF() failed'

###############################################################################
#This func is to calculate the profit points buying and selling using MA signal
#This method is using data from disk file directly.
#Args:
#codename   - stock index, such as SH000001
#malength   - MA cycle
#startdate  - start date of the time interval
#enddate    - end date of the time interval
#Return value:
#   total profit
#   Note: return False if not match calc/searching criteria
def transact_MAs_fromDF_byTimeInterval(codename, malength, startdate, enddate):
    
    try:

        workdata = data_opsrc_dir + "\\" + codename + '.data'

        #check the line number, if linenum < malength, then just return
        fd = open(workdata, 'r')
        aList = fd.readlines()
        linenum = len(aList)
        fd.close()
        if linenum < malength:
            print('No enough line records in source data file to execute MA calculation.')
            return False
        
        #check if startdate is in aList
        sdfound = False
        for line in aList:
            wd = []
            llist = line.strip().split()
            
            for i in llist:
                if i != '':
                    wd.append(i)

            date = wd[0]

            if startdate == date:
                sdfound = True
                #print ('Start date found, move on...')
                break
            
        if not sdfound: #startdate not found, just return
            print ('Start date not found, just return...')
            return False
            
        #check if enddate is in aList
        edfound = False
        for line in aList:
            wd = []
            llist = line.strip().split()
            
            for i in llist:
                if i != '':
                    wd.append(i)

            date = wd[0]
            
            if enddate == date:
                edfound = True
                #print ('End date found, move on...')
                break
            
        if not edfound: #enddate not found, just return
            print ('End date not found, just return...')
            return False

        #table tuple list for closing and its date          
        closing_list    = []
        date_list       = []
        #list for buyin point and sellout point
        #format: [date, closing]
        buyin_point     = []
        sellout_point   = []
        earned_points   = [] # earnedpoint = selloutpoint - buyinpoint
        buyin_sellout_earned_list = [] # format: [[], [], []]
        #total profit during the interval
        #tp[0] - total absolute value
        #tp[1] - total percent 
        total_profit = []
        #summary list
        summaryList = []
        
        #InAll
        #status -> position status
        #action -> position action
        #status:    longpostn   shortpostn
        #action:    buyin       sellout     onhold
        #status:
        #   0 (shortpostn or empty position)
        #   1 (longpostn or full position)
        #action:
        #   0 (onhold)  -> status no change
        #   1 (buying)  -> status = 1
        #   2 (sellout) -> status = 0
        status = 0
        action = 0

        #First, find the index of startdate and enddate
        #Also build the closing list and date list
        #Here the closing list is from indx 0 to enddate indx
        #sindx - index of the start date
        #eindx - index of the end date
        sindx = 0
        eindx = 0
        indx = 0
        for line in open(workdata, 'r'):
            
            wd = []
            llist = line.strip().split()
            
            for i in llist:
                if i != '':
                    wd.append(i)

            date    =   wd[0]
            closing = float(wd[4])
            closing_list.append(closing)
            date_list.append(date)
                    
            if date == startdate:
                sindx = indx

            if date == enddate:
                eindx = indx
                break

            indx = indx + 1

##        #if there is no enough data to calc MAs, just return 
##        date_interval = eindx - sindx
##        if sindx < malength:
##            print('Start date not able to calc MA.')
##            return False

        #Second, process data in this interval
        #At this point, we got start indx and end indx
        indx = 0
        for cl in closing_list:
            
            if indx < malength -1:
                #there is no ma value so far
                indx = indx + 1
                continue

            if indx < sindx:
                #earlier than start date, just continue
                indx = indx + 1
                continue

            if indx > eindx:
                #later than end date, just break out
                break
            
            #Now calc MA of malength
            mavalue   = calc_MA(closing_list, indx, malength)

            closing = closing_list[indx]
            date    = date_list[indx]

            #main algorithm
            if closing >= mavalue:
                
                #either buyin or onhold
                if status == 0:
                    #need to buyin and record the date and closing value at this buyin time
                    action = 1
                    status = 1
                    buyin_point = [date, closing, mavalue]
                    
                elif status == 1: #full position already
                    #need to just onhold, status no change
                    action = 0
                    status = 1

            elif closing < mavalue:
                
                #either sellout or onhold
                if status == 0:
                    # need to onhold, status no change
                    action = 0
                    status = 0

                elif status == 1:
                    # need to sellout, then calc the earned points (maybe positive or negetive)
                    action = 2
                    status = 0
                    sellout_point   = [date, closing, mavalue]
                    earnedpoints    = sellout_point[1] - buyin_point[1]
                    earned_points   = [date, earnedpoints]
                    buyin_sellout_earned_list.append([buyin_point, sellout_point, earned_points])
            
            indx = indx + 1

        #after the main algorithm, here we come to the last value
        #need to check if the last value is larger than mavalue
        #Here if status == 1, means there is only buyin_point and there is no sellout_point
        #So we use the last closing value as sellout_point
        #Of course there is chance that sellout date is same as buyin date...
        #For the profit of the last value, 3 solutions:
        #   1) selloutpoint - buyinpoint
        #   2) mavalue - buyinpoint
        #   3)( mavalue + (selloutpoint - mavalue)/2 ) - buyinpoint
        if status == 1:
            sellout_point = [date, closing, mavalue]
            #earnedpoints    = sellout_point[1] - buyin_point[1]
            #earnedpoints    = mavalue - buyin_point[1]
            earnedpoints    = (mavalue + sellout_point[1])/2 - buyin_point[1]
            earned_points   = [date, earnedpoints]
            buyin_sellout_earned_list.append([buyin_point, sellout_point, earned_points])                     

        #SUMMARY
        #buyin date; buyin point; sellout date; sellout point; profit; profit percent
        #calc total profit and total profit percent at the same time; both are float number
        #For total profit point, it could not reflect the real profit due to stock price is fluctuating
        #For example, buyin at 5, sellout at 10, we got 10-5=5 point;
        #another buyin at 10, sellout at 15, we got 15-10=5 point. The point is same but net profit is different (100% vs 50%)
        #If we use percent and sum them up, we have assumption that everytime we buyin we use same initial capital/money.
        #In this case we haven't count in the profit of profit...
        #If we count all of profit on profit in, here profit percent is:
        #A*(1+pp1)  A(1+pp1)(1+pp2) A(1+pp1)(1+pp2)(1+pp3) ...; here A is initial capital, ppx is the profit percent of each transcation.
        #Here we use a compromised solution, we use 50% of profit and add it into initial capital everytime
        #This means we will have (1 + profit_percent/2)*IC everytime...
        total_profit_point = 0.0
        total_profit_percent = 0.0
        outfile = data_procd_dir + "\\" + codename + ".out"
        fd = open(outfile, 'a')
        fd.write('The data for MA'+str(malength)+':\n')
        #fd.write('buyin date;   buyin point;    buyin mavalue;      sellout date;    sellout point;     sellout mavalue;     profit;     profit percent\n')
        for bse in buyin_sellout_earned_list:
            buyin_date      = bse[0][0]
            buyin_point     = bse[0][1]
            buyin_mavalue   = bse[0][2]
            sellout_date    = bse[1][0]
            sellout_point   = bse[1][1]
            sellout_mavalue = bse[1][2]
            profit_point    = bse[2][1]
            profit_percent  = profit_point/buyin_point
            total_profit_point   = total_profit_point + profit_point
            #total_profit_percent = total_profit_percent + profit_percent
            #total_profit_percent = (1 + total_profit_percent)*(1 + profit_percent) - 1
            total_profit_percent = total_profit_percent + profit_percent + total_profit_percent*profit_percent
            #total_profit_percent = total_profit_percent + ( profit_percent/2 + 1 )
            fd.write(buyin_date + ';    \t' + str(buyin_point) + ';     \t' + str(buyin_mavalue) + ';   \t' + sellout_date + ';     \t' + str(sellout_point) + ';   \t' + str(sellout_mavalue) + ';     \t' + str(profit_point) + ';    \t' + str(profit_percent) + '\n')
        fd.close()

       #return summaryList
        total_profit = [total_profit_point, total_profit_percent]
        return total_profit
        
        
    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'transact_MAs_fromDF_byTimeInterval() failed'
    
###############################################################################
#This func is to calculate the profit points buying and selling using MA signal
#This method is is wrapper of transaction by DB and DF
#this func is for single Stock Index
def profit_MAs_SSI(codename, malength):
    
    try:
        #write summary file header before processing
        outfile = data_procd_dir + "\\" + codename + ".out"
        fd = open(outfile, 'a')
        fd.write('buyin date; buyin point; buyin mavalue; sellout date; sellout point; sellout mavalue; profit; profit percent\n')
        fd.close()
        
        ml = 5
        while ml <= malength:
            #total profit; [total_profit_point, total_profit_percent]
            tp = transact_MAs_fromDF(codename, ml)
            print('Total profit by MA' + str(ml) + ' is ' + str(tp[0]) + '    \t' + str(tp[1]))
            ml = ml + 1
    
    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'profit_MAs_SSI() failed'

###############################################################################
#This func is to calculate the profit points buying and selling using MA signal
#Time internval is used here
#This method is is wrapper of transaction by DB and DF
#transact_MAs_fromDF_byTimeInterval(codename, malength, startdate, enddate)
#this func is for single Stock Index
def profit_MAs_SSI_byTI(codename, malength, startdate, enddate):
    
    try:
        #write summary file header before processing
        outfile = data_procd_dir + "\\" + codename + ".out"
        fd = open(outfile, 'a')
        fd.write('buyin date; buyin point; buyin mavalue; sellout date; sellout point; sellout mavalue; profit; profit percent\n')
        fd.close()
        
        ml = 5
        while ml <= malength:
            #total profit; [total_profit_point, total_profit_percent]
            tp = transact_MAs_fromDF_byTimeInterval(codename, ml, startdate, enddate)
            
            if tp == False:
                print ('No enough data to calc MAs in sub function, just break.')
                break

            print('Total profit by MA' + str(ml) + ' from ' + startdate + ' to ' + enddate + ' is ' + str(tp[0]) + '    \t' + str(tp[1]))
            ml = ml + 1
    
    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'profit_MAs_SSI_byTI() failed'
    
###############################################################################
#This func is to calculate the profit points buying and selling using MA signal
#This method is is wrapper of transaction by DB and DF
#this func is for multiple Stock Index
#codename - stock index code, such as 399006
#  In this func this value is not used.
#  The func will scan all files in data_opsrc_dir then calc them all.
#malength - the upper bound of MA. If it's 60, means we would calc MA by 5 ~ 60
#  Implicit lower bound of MA is 5.
def profit_MAs_MSI(codename, malength):
    
    try:

        sfList = os.listdir(data_opsrc_dir)

        for tmpfn in sfList:

            #stock index name, such as SH000001
            codename = tmpfn.split('.')[0]
            
            #write summary file header before processing
            outfile = data_procd_dir + "\\" + codename + ".out"
            fd = open(outfile, 'a')
            fd.write('buyin date; buyin point; buyin mavalue; sellout date; sellout point; sellout mavalue; profit; profit percent\n')
            fd.close()
        
            print ('------------------------------------')
            print ('Now we calc MA for ' + codename)

            #summary list, format: [[MAx, total_profit]...]
            sumList = []     
            ml = 5
            while ml <= malength:
                
                tp = transact_MAs_fromDF(codename, ml)
                print('Total profit by MA' + str(ml) + ' is ' + str(tp[0]) + '  \t' + str(tp[1]))
                
                sumList.append( ['MA'+str(ml), tp] )
                ml = ml + 1

            #profilt summary
            #file: SUMMARY.out
            sumfile = data_procd_dir + "\\" + "SUMMARY.out"
            fd = open(sumfile, 'a')
            fd.write('------------------------------------\n')
            fd.write('Summary data for '+codename+':\n')
            for sl in sumList:
                fd.write(sl[0] + '              \t' + str(sl[1][0]) + '             \t' + str(sl[1][1]) + '\n')
            fd.close()
    
    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'profit_MAs_MSI() failed'

###############################################################################
#This func is to do cleanup work before data processing
#It will:
#   1) remove all files in 
def cleanup():
    
    try:
        #cleanup file list
        cufList = os.listdir(data_procd_dir)
        
        for tmpfn in cufList:
            fn = data_procd_dir + "\\" + tmpfn
            os.remove(fn)
    
    except Exception, exc:
        print(traceback.format_exc())
	raise Exception, 'cleanup() failed'

###############################################################################
####GLOBALS
##home_dir = "D:\\Personal_Misc\\0000_Investment\\QuantitativeModel\\"
##data_export_dir = home_dir + "export"
##data_ipsrc_dir = home_dir + "ipsrcdata"
##data_opsrc_dir = home_dir + "opsrcdata"
##data_procd_dir = home_dir + "procddata"
###############################################################################
# MAIN:
###############################################################################
def main(argv):
    
    try:

        #procSrcData()
        
        #importDataAll_MAs()
        
        #profit_MAs('SZ399006', 60)
        
        cleanup()
        
        #profit_MAs_SSI('SZ399006', 60)
        #profit_MAs_SSI('SZ159915', 60)
        
        #profit_MAs_MSI('all', 60)

        #profit_MAs_SSI_byTI('SZ300393', 60, '2014-11-18', '2015-11-13')
        #profit_MAs_SSI_byTI('SZ300448', 60, '2015-06-01', '2015-11-13')
        #profit_MAs_SSI_byTI('SZ399006', 60, '2014-01-02', '2015-11-13')
        profit_MAs_SSI_byTI('SZ159915', 60, '2014-01-02', '2015-11-13')

    except KeyboardInterrupt:
	    print ('User interrupt to stop command!')
	    sys.exit(1)

    except Exception, exc:
    #except Exception as exc:   python3
	    print (str(exc))
	    print ('Command failed!')
	    sys.exit(1)

# Execute main
if __name__ == '__main__':
    main(sys.argv[1:])
