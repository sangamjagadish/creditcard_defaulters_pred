from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandracsv import CassandraCsv
import pandas as pd
import shutil
import os
from os import listdir
from Application_Logging.logger import App_Logger


class DBOperation:
    """
    This class shall be used for handling all the Cassandra Operations.
    """

    def __init__(self):
        self.badFilePath = "Training_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()
        self.keyspace = 'training1'

    def dataBaseConnection(self):

        """
        Description: This function will make the connection to Cassandra Database.
        Output: Connection to DB
        On Failure: Raise ConnectionError
        """

        try:
            cloud_config = {'secure_connect_bundle': './secure-connect-creditcardproject.zip'}
            auth_provider = PlainTextAuthProvider('tUCZRspebWHpHxxoFZTvFceF',
                                                  '-jKA_8DCZcRh6o-LGO,FIkP2DPRKUj6lkPL8DTmRmu5TMCaZn1hSorlCW.k9G_t8YzJjkmXH4-2g,GLqIXXU6wokDvJ5ZSjc1kLpTQTQd+wsgd.YA+ERpfU3YAZucF-b')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            self.logger.log(file, "Database is connected")
            file.close()

        except ConnectionError:
            file = open('Training_Logs/DataBaseConnection.txt', 'a+')
            self.logger.log(file, "Error while connecting to Database:: %s" % ConnectionError)
            file.close()
            raise ConnectionError

        return session

    def createTableDB(self, keyspace):

        """
        Description: This method creates the table in the given DB which will be used to insert the Good data after raw data validation.
        Output: None
        On Failure: Raise Exception
        """

        session = self.dataBaseConnection()
        session.execute("USE %s;" % keyspace)
        goodFilePath = self.goodFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Training_Logs/DbTableCreateLog.txt", 'a+')

        for file in onlyfiles:
            df = pd.read_csv(goodFilePath + '/' + file)
            cols = [i[1] for i in enumerate(df)]
            try:
                query = 'DROP TABLE IF EXISTS Good_Raw_Data;'
                session.execute(query)

                query = f"""CREATE TABLE IF NOT EXISTS Good_Raw_Data(id int  PRIMARY KEY, "{cols[0]}" Int,"{cols[1]}" Int,"{cols[2]}" Int,"{cols[3]}" Int,
                                        "{cols[4]}" Int,"{cols[5]}" Int,"{cols[6]}" Int,"{cols[7]}" Int,"{cols[8]}" Int,"{cols[9]}" Int,"{cols[10]}" Int,"{cols[11]}" Int,"{cols[12]}" Int,"{cols[13]}" Int,"{cols[14]}" Int,
                                        "{cols[15]}" Int,"{cols[16]}" Int,"{cols[17]}" Int,"{cols[18]}" Int,"{cols[19]}" Int,"{cols[20]}" Int,"{cols[21]}" Int,"{cols[22]}" Int,"{cols[23]}" Int);"""
                session.execute(query)

                file = open("Training_Logs/DBTableCreateLog.txt", 'a+')
                self.logger.log(log_file, "Table Good_Raw_Data created successfully")
                file.close()

            except Exception as e:
                file = open("Training_Logs/TableDBCreateLog.txt", 'a+')
                self.logger.log(log_file, "Error occured while creating Table Good_Raw_Data: %s" % e)
                file.close()
                file = open("Training_Logs/DatabaseConnectionLog.txt", 'a+')
                self.logger.log(log_file, "%s Database disconnected successfully!!" % keyspace)
                file.close()
                raise e

        session.shutdown()

    def insertIntoTableGoodData(self, keyspace):

        """
        Description: This method inserts the Good data files from the Good_Raw folder into the
                     table of Database.
        Output: None
        On Failure: Raise Exception
        """

        session = self.dataBaseConnection()
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        session.execute("USE %s;" % keyspace)
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:

            try:
                df = pd.read_csv(goodFilePath + '/' + file)
                cols = [i[1] for i in enumerate(df)]
                list_ = df.values.tolist()
                values = [i for i in list_]
                for i in range(len(values)):
                    try:
                        query = f"""INSERT INTO Good_Raw_Data(id,"{cols[0]}","{cols[1]}","{cols[2]}","{cols[3]}","{cols[4]}","{cols[5]}","{cols[6]}","{cols[7]}","{cols[8]}",
                        "{cols[9]}","{cols[10]}","{cols[11]}","{cols[12]}","{cols[13]}","{cols[14]}","{cols[15]}","{cols[16]}","{cols[17]}","{cols[18]}","{cols[19]}","{cols[20]}","{cols[21]}",
                        "{cols[22]}","{cols[23]}") VALUES({i},{values[i][0]},{values[i][1]},{values[i][2]},{values[i][3]},{values[i][4]},{values[i][5]},{values[i][6]},{values[i][7]},{values[i][8]},
                        {values[i][9]},{values[i][10]},{values[i][11]},{values[i][12]},{values[i][13]},{values[i][14]},{values[i][15]},{values[i][16]},{values[i][17]},{values[i][18]},{values[i][19]},{values[i][20]},
                        {values[i][21]},{values[i][22]},{values[i][23]});"""
                        session.execute(query)
                        self.logger.log(log_file, "%s: Data inserted into table successfully!!" % file)
                    except Exception as e:
                        raise e

            except Exception as e:

                self.logger.log(log_file, "Error while inserting data into table: %s" % e)
                shutil.move(goodFilePath + '/' + file, badFilePath)
                self.logger.log(log_file, "File Move Successfully %s" % file)
                log_file.close()

        session.shutdown()
        log_file.close()

    def selectingDatafromtableintocsv(self, keyspace):

        """
        Description: This method exports the data from table of Database to csv file in a given location.
        Output: None
        On Failure: Raise Exception
        """

        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        session = self.dataBaseConnection()
        session.execute("USE %s;" % keyspace)

        try:

            query = "SELECT * FROM Good_Raw_Data;"
            result = session.execute(query)

            # Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Exporting to csv file from Database
            CassandraCsv.export(result, output_dir=self.fileFromDb, filename=self.fileName)

            # Sorting rows and columns and exporting to csv file
            sortfile = pd.read_csv(self.fileFromDb + self.fileName)
            sortfile = sortfile.sort_values(['Id']).drop('Id', axis=1)
            sortfile = sortfile[
                ['Age','Bill_Amt1','Bill_Amt2','Bill_Amt3','Bill_Amt4','Bill_Amt5','Bill_Amt6','Education','Limit_Bal','Marriage','Pay_0','Pay_2','Pay_3','Pay_4','Pay_5','Pay_6','Pay_Amt1','Pay_Amt2','Pay_Amt3','Pay_Amt4','Pay_Amt5','Pay_Amt6','Sex','Default Payment Next Month']]
            sortfile.to_csv(self.fileFromDb + self.fileName, index=False)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, " File export failed. Error: %s" % e)
            log_file.close()

        session.shutdown()


