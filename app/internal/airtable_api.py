import os
import pyairtable
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_API_TOKEN = os.environ['AIRTABLE_API_TOKEN']
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_PROCESSORS_TABLE_ID = os.environ["AIRTABLE_PROCESSORS_TABLE_ID"]
# airtable = pyairtable.Airtable(AIRTABLE_API_TOKEN)
api = Api(AIRTABLE_API_TOKEN)

def getTables(base_id):
    base = api.base(base_id)
    tables = base.tables()
    return tables

def getTableContents(base_id, table_name):
    base = api.base(base_id)
    tables = base.tables()
    ## Is there a better way than to just loop?
    for table in tables:
        ## Each table has base, name, id
        ## table.all() returns table contents
        if table.name == table_name:
            return table.all()

table_contents = getTableContents(AIRTABLE_BASE_ID, "IL_Ver_A_Well_Completion")
# print(table_contents)