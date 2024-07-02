import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.context import DynamicFrame
import time

# Get the necessary arguments from the command line
args = getResolvedOptions(sys.argv, ['TempDir', 'JOB_NAME', 'sourcedatabase', 'destinationpath', 'region'])

# Initialize a Spark context
sc = SparkContext()

# Initialize a Glue context
glueContext = GlueContext(sc)

# Get the Spark session from the Glue context
spark = glueContext.spark_session

# Initialize a Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Extract arguments for source database, destination path, and region
sourcedatabase = args['sourcedatabase']
destinationpath = args['destinationpath']
region = args['region']

# Initialize a Glue client for the specified region
glue = boto3.client('glue', region_name=region)

# Get the list of tables from the source database
response = glue.get_tables(DatabaseName=sourcedatabase)

# Check if the response contains a list of tables
if 'TableList' in response:
    # Iterate over each table in the list
    for table in response['TableList']:
        # Get the name of the source table
        sourcetable = table['Name']

        # Create a DynamicFrame from the source table
        datasource0 = glueContext.create_dynamic_frame.from_catalog(database=sourcedatabase, table_name=sourcetable, transformation_ctx="datasource0")

        # Check if the DynamicFrame contains any data
        if datasource0.toDF().head(1):
            # Write the DynamicFrame to the specified S3 path in Parquet format
            datasink = glueContext.write_dynamic_frame.from_options(frame=datasource0, connection_type="s3", connection_options={"path": destinationpath + sourcetable}, format="parquet", transformation_ctx="datasink4")

# Commit the Glue job
job.commit()
