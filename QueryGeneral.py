import pandas as pd
#import pyodbc
from turbodbc import connect, make_options
from sqlalchemy import event, create_engine
from datetime import date, timedelta, datetime

class Query:
    def __init__(self,  params, start_date=date.today(), meter=None, end_date=None, kw=None):
        self.cursor = None
        self.meter = meter
        self.start_date = start_date
        self.end_date = end_date
        if 'database' in params:
            self.database = params['database']
        self.uid = params['uid']
        self.pwd = params['pwd']
        self.server = params['server']
        if 'engine' in params:
            self.engine = params['engine']
        if ('database' in params) and ('engine' in params):
            self.connect_to_db()
        if kw:
            self.database = params['database_' + kw]
            self.engine = params['engine_' + kw]
        self.connect_to_db()
        self.misc_operations()
        @event.listens_for(self.engine, 'before_cursor_execute')
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                self.cursor.fast_executemany = True

    def connect_to_db(self):
        options = make_options(prefer_unicode=True,
                               autocommit=True,
                               use_async_io=True)
        cnxn = connect(
                DRIVER="SQL SERVER",
                SERVER=self.server,
                DATABASE=self.database,
                UID=self.uid,
                PWD=self.pwd,
                turbodbc_options=options
        )
        self.conn = cnxn
        self.cursor = cnxn.cursor()
        self.engine = create_engine(self.engine)

    def misc_operations(self):
        q1 = "SET NOCOUNT ON;"
        self.cursor.execute(q1)
        self.cursor.execute("USE ?", self.database)

    def query_to_df(self, query):
        df = pd.read_sql_query(query, self.conn)
        return df

    def write_df_to_db(self, df, db_name, if_exists=None):
        if if_exists == None:
            if_exists = 'append'
        columns_ = '(' + ", ".join(df.columns) + ")"
        value_place_holders = ['?' for col in df.columns]
        sql_val = "(" + ", ".join(value_place_holders) + ")"
        if self.schema:
            query = f"""
                INSERT INTO {schema}.{db_name} {columns_}
                    VALUES {sql_val}
                """
        else:
            query = f"""
                INSERT INTO {db_name} {columns_}
                    VALUES {sql_val}
                """
            val_array = [df[col].values for col in df.columns]
            if if_exists == 'replace':
                self.cursor.execute(f"delete from {schema}.{db_name}")
            self.cursor.executemanycolumns(query, val_array)


            """df.to_sql(name=db_name,
                  schema=self.schema,
                  con=self.engine,
                  if_exists='append',
                  chunksize=50000,
                  index=False)
                else:
                    df.to_sql(name=db_name,
                              con=self.engine,
                              if_exists=if_exists,
                              chunksize=50000,
                              index=False)
             """
        #https://erickfis.github.io/loose-code/

    def delete_table(self, table_name, schema):
        self.cursor.execute("TRUNCATE TABLE {}.{}".format(schema, table_name))
        #self.conn.commit()
        pass

    def execute_query(self, query):
        self.cursor.execute(query)
        #self.conn.commit()
