# this program loads Census ACS data using basic, slow INSERTs 
# run it with -h to see the command line options
import io
import time
import psycopg2
import psycopg2.extras
import argparse
import re
import csv

DBname = "postgres"
DBuser = "postgres"
DBpwd = "pass"
TableName = 'CensusData'
Datafile = "filedoesnotexist"  # name of the data file to be loaded
CreateDB = True  # indicates whether the DB table should be (re)-created

def row2vals(row):
  for key in row:
    if not row[key]:
      row[key] = 0  # ENHANCE: handle the null vals
    row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

  ret = f"""
     {row['CensusTract']},            -- CensusTract
     '{row['State']}',                -- State
     '{row['County']}',               -- County
     {row['TotalPop']},               -- TotalPop
     {row['Men']},                    -- Men
     {row['Women']},                  -- Women
     {row['Hispanic']},               -- Hispanic
     {row['White']},                  -- White
     {row['Black']},                  -- Black
     {row['Native']},                 -- Native
     {row['Asian']},                  -- Asian
     {row['Pacific']},                -- Pacific
     {row['Citizen']},                -- Citizen
     {row['Income']},                 -- Income
     {row['IncomeErr']},              -- IncomeErr
     {row['IncomePerCap']},           -- IncomePerCap
     {row['IncomePerCapErr']},        -- IncomePerCapErr
     {row['Poverty']},                -- Poverty
     {row['ChildPoverty']},           -- ChildPoverty
     {row['Professional']},           -- Professional
     {row['Service']},                -- Service
     {row['Office']},                 -- Office
     {row['Construction']},           -- Construction
     {row['Production']},             -- Production
     {row['Drive']},                  -- Drive
     {row['Carpool']},                -- Carpool
     {row['Transit']},                -- Transit
     {row['Walk']},                   -- Walk
     {row['OtherTransp']},            -- OtherTransp
     {row['WorkAtHome']},             -- WorkAtHome
     {row['MeanCommute']},            -- MeanCommute
     {row['Employed']},               -- Employed
     {row['PrivateWork']},            -- PrivateWork
     {row['PublicWork']},             -- PublicWork
     {row['SelfEmployed']},           -- SelfEmployed
     {row['FamilyWork']},             -- FamilyWork
     {row['Unemployment']}            -- Unemployment
  """

  return ret


def initialize():
  parser = argparse.ArgumentParser()
  parser.add_argument("-d", "--datafile", required=True)
  parser.add_argument("-c", "--createtable", action="store_true")
  args = parser.parse_args()

  global Datafile
  Datafile = args.datafile
  global CreateDB
  CreateDB = args.createtable

# read the input data file into a list of row strings
def readdata(fname):
  print(f"readdata: reading from File: {fname}")
  with open(fname, mode="r") as fil:
    dr = csv.DictReader(fil)
    
    rowlist = []
    for row in dr:
      rowlist.append(row)

  return rowlist

# convert list of data rows into list of SQL 'INSERT INTO ...' commands
def getSQLcmnds(rowlist):
  cmdlist = []
  for row in rowlist:
    valstr = row2vals(row)
    cmd = f"INSERT INTO {TableName}_TEMP VALUES ({valstr});"
    cmdlist.append(cmd)
  return cmdlist

# connect to the database
def dbconnect():
  connection = psycopg2.connect(
    host="localhost",
    database=DBname,
    user=DBuser,
    password=DBpwd,
  )
  connection.autocommit = True
  return connection

# create the target table 
# assumes that conn is a valid, open connection to a Postgres database
def createTable(conn):

  with conn.cursor() as cursor:
    cursor.execute(f"""
      DROP TABLE IF EXISTS {TableName};
      CREATE TABLE {TableName} (
        CensusTract         NUMERIC,
        State               TEXT,
        County              TEXT,
        TotalPop            INTEGER,
        Men                 INTEGER,
        Women               INTEGER,
        Hispanic            DECIMAL,
        White               DECIMAL,
        Black               DECIMAL,
        Native              DECIMAL,
        Asian               DECIMAL,
        Pacific             DECIMAL,
        Citizen             DECIMAL,
        Income              DECIMAL,
        IncomeErr           DECIMAL,
        IncomePerCap        DECIMAL,
        IncomePerCapErr     DECIMAL,
        Poverty             DECIMAL,
        ChildPoverty        DECIMAL,
        Professional        DECIMAL,
        Service             DECIMAL,
        Office              DECIMAL,
        Construction        DECIMAL,
        Production          DECIMAL,
        Drive               DECIMAL,
        Carpool             DECIMAL,
        Transit             DECIMAL,
        Walk                DECIMAL,
        OtherTransp         DECIMAL,
        WorkAtHome          DECIMAL,
        MeanCommute         DECIMAL,
        Employed            INTEGER,
        PrivateWork         DECIMAL,
        PublicWork          DECIMAL,
        SelfEmployed        DECIMAL,
        FamilyWork          DECIMAL,
        Unemployment        DECIMAL
      );	
      
    """)

    print(f"Created {TableName}")

def create_unlogged_table(conn):
  with conn.cursor() as cursor:
    cursor.execute(f"""
      DROP TABLE IF EXISTS {TableName}_UNLOGGED;
      CREATE UNLOGGED TABLE {TableName}_UNLOGGED (
        CensusTract         NUMERIC,
        State               TEXT,
        County              TEXT,
        TotalPop            INTEGER,
        Men                 INTEGER,
        Women               INTEGER,
        Hispanic            DECIMAL,
        White               DECIMAL,
        Black               DECIMAL,
        Native              DECIMAL,
        Asian               DECIMAL,
        Pacific             DECIMAL,
        Citizen             DECIMAL,
        Income              DECIMAL,
        IncomeErr           DECIMAL,
        IncomePerCap        DECIMAL,
        IncomePerCapErr     DECIMAL,
        Poverty             DECIMAL,
        ChildPoverty        DECIMAL,
        Professional        DECIMAL,
        Service             DECIMAL,
        Office              DECIMAL,
        Construction        DECIMAL,
        Production          DECIMAL,
        Drive               DECIMAL,
        Carpool             DECIMAL,
        Transit             DECIMAL,
        Walk                DECIMAL,
        OtherTransp         DECIMAL,
        WorkAtHome          DECIMAL,
        MeanCommute         DECIMAL,
        Employed            INTEGER,
        PrivateWork         DECIMAL,
        PublicWork          DECIMAL,
        SelfEmployed        DECIMAL,
        FamilyWork          DECIMAL,
        Unemployment        DECIMAL
      );	
      
    """)

def create_temporary_table(conn):
  with conn.cursor() as cursor:
    cursor.execute(f"""
      DROP TABLE IF EXISTS {TableName}_TEMP;
      CREATE TEMP TABLE {TableName}_TEMP (
        CensusTract         NUMERIC,
        State               TEXT,
        County              TEXT,
        TotalPop            INTEGER,
        Men                 INTEGER,
        Women               INTEGER,
        Hispanic            DECIMAL,
        White               DECIMAL,
        Black               DECIMAL,
        Native              DECIMAL,
        Asian               DECIMAL,
        Pacific             DECIMAL,
        Citizen             DECIMAL,
        Income              DECIMAL,
        IncomeErr           DECIMAL,
        IncomePerCap        DECIMAL,
        IncomePerCapErr     DECIMAL,
        Poverty             DECIMAL,
        ChildPoverty        DECIMAL,
        Professional        DECIMAL,
        Service             DECIMAL,
        Office              DECIMAL,
        Construction        DECIMAL,
        Production          DECIMAL,
        Drive               DECIMAL,
        Carpool             DECIMAL,
        Transit             DECIMAL,
        Walk                DECIMAL,
        OtherTransp         DECIMAL,
        WorkAtHome          DECIMAL,
        MeanCommute         DECIMAL,
        Employed            INTEGER,
        PrivateWork         DECIMAL,
        PublicWork          DECIMAL,
        SelfEmployed        DECIMAL,
        FamilyWork          DECIMAL,
        Unemployment        DECIMAL
      );	
      
    """)

def executeBatchCmd(rowList, conn):
  for row in rowList:
    for key in row:
      if not row[key]:
        row[key] = 0  # ENHANCE: handle the null vals
      row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals

  with conn.cursor() as cursor:
    print(f"Loading {len(rowList)} rows")
    start = time.perf_counter()

    psycopg2.extras.execute_batch(cursor, """INSERT INTO CensusData VALUES (
     %(CensusTract)s,            -- CensusTract
     %(State)s,                -- State
     %(County)s,               -- County
     %(TotalPop)s,               -- TotalPop
     %(Men)s,                    -- Men
     %(Women)s,                  -- Women
     %(Hispanic)s,               -- Hispanic
     %(White)s,                  -- White
     %(Black)s,                  -- Black
     %(Native)s,                 -- Native
     %(Asian)s,                  -- Asian
     %(Pacific)s,                -- Pacific
     %(Citizen)s,                -- Citizen
     %(Income)s,                 -- Income
     %(IncomeErr)s,              -- IncomeErr
     %(IncomePerCap)s,           -- IncomePerCap
     %(IncomePerCapErr)s,        -- IncomePerCapErr
     %(Poverty)s,                -- Poverty
     %(ChildPoverty)s,           -- ChildPoverty
     %(Professional)s,           -- Professional
     %(Service)s,                -- Service
     %(Office)s,                 -- Office
     %(Construction)s,           -- Construction
     %(Production)s,             -- Production
     %(Drive)s,                  -- Drive
     %(Carpool)s,                -- Carpool
     %(Transit)s,                -- Transit
     %(Walk)s,                   -- Walk
     %(OtherTransp)s,            -- OtherTransp
     %(WorkAtHome)s,             -- WorkAtHome
     %(MeanCommute)s,            -- MeanCommute
     %(Employed)s,               -- Employed
     %(PrivateWork)s,            -- PrivateWork
     %(PublicWork)s,             -- PublicWork
     %(SelfEmployed)s,           -- SelfEmployed
     %(FamilyWork)s,             -- FamilyWork
     %(Unemployment)s            -- Unemployment
    );""", rowList)

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')

def clean_csv_value(value) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

def copy_stringio(rowList, conn) -> None:
    for row in rowList:
      for key in row:
        if not row[key]:
          row[key] = 0  # ENHANCE: handle the null vals
        row['County'] = row['County'].replace('\'','')  # TIDY: eliminate quotes within literals
    with conn.cursor() as cursor:
        csv_file_like_object = io.StringIO()
        for row in rowList:
            csv_file_like_object.write('|'.join(map(clean_csv_value, (
              row['CensusTract'],            
              row['State'],                
              row['County'],               
              row['TotalPop'],               
              row['Men'],                    
              row['Women'],                  
              row['Hispanic'],               
              row['White'],                  
              row['Black'],                  
              row['Native'],                 
              row['Asian'],                  
              row['Pacific'],                
              row['Citizen'],                
              row['Income'],                 
              row['IncomeErr'],              
              row['IncomePerCap'],           
              row['IncomePerCapErr'],        
              row['Poverty'],                
              row['ChildPoverty'],           
              row['Professional'],           
              row['Service'],                
              row['Office'],                 
              row['Construction'],           
              row['Production'],             
              row['Drive'],                  
              row['Carpool'],                
              row['Transit'],                
              row['Walk'],                   
              row['OtherTransp'],            
              row['WorkAtHome'],             
              row['MeanCommute'],            
              row['Employed'],               
              row['PrivateWork'],            
              row['PublicWork'],             
              row['SelfEmployed'],           
              row['FamilyWork'],             
              row['Unemployment']            
            ))) + '\n')
        csv_file_like_object.seek(0)
        cursor.copy_from(csv_file_like_object, 'censusdata', sep='|')

def create_constraints(conn):
  with conn.cursor() as cursor:
    cursor.execute(f"""ALTER TABLE {TableName} ADD PRIMARY KEY (CensusTract);
      CREATE INDEX idx_{TableName}_State ON {TableName}(State);""")

def append_staging_table(conn):
  with conn.cursor() as cursor:
    cursor.execute(f"""INSERT INTO {TableName} SELECT * FROM {TableName}_TEMP;""")

def load(conn, icmdlist):

  with conn.cursor() as cursor:
    print(f"Loading {len(icmdlist)} rows")
    start = time.perf_counter()
  
    for cmd in icmdlist:
      cursor.execute(cmd)

    elapsed = time.perf_counter() - start
    print(f'Finished Loading. Elapsed Time: {elapsed:0.4} seconds')


def main():
  initialize()
  conn = dbconnect()
  rlis = readdata(Datafile)
  #cmdlist = getSQLcmnds(rlis)

  if CreateDB:
    createTable(conn)

  copy_stringio(rlis, conn)
  create_constraints(conn)


if __name__ == "__main__":
  main()
