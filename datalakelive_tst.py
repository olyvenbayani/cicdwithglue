import unittest
import sys
import boto3
import filecmp
import time

# Initialize AWS clients for Glue, CloudFormation, Athena, and S3
glue = boto3.client('glue')
client = boto3.client('cloudformation')
athena = boto3.client('athena')
s3 = boto3.client('s3')

# Function to get resources from a CloudFormation stack
def getStackResources(stackname):
    response = client.describe_stack_resources(StackName=stackname)
    return response['StackResources']

# Function to delete a Glue database
def deleteDatabase(databasename):
    try:
        glue.delete_database(Name=databasename)
    except:
        print("table " + databasename + " did not exist")

# Function to get the database name of a Glue crawler
def getcrawlerDatabase(crawlername):
    response = glue.get_crawler(Name=crawlername)
    return response['Crawler']['DatabaseName']

# Function to run a Glue crawler and wait for it to finish
def runCrawler(crawlername):
    glue.start_crawler(Name=crawlername)
    response = glue.get_crawler(Name=crawlername)
    state = response['Crawler']['State']
    
    # Wait for the crawler to stop running
    while state == 'RUNNING' or state == 'STOPPING':
        time.sleep(60)
        response = glue.get_crawler(Name=crawlername)
        state = response['Crawler']['State']
    
    print("final state {}".format(state))
    print("last crawl {}".format(response['Crawler']['LastCrawl']['Status']))
    
    return response['Crawler']['LastCrawl']['Status']

# Function to run a Glue job and wait for it to finish
def runJob(jobname):
    response = glue.start_job_run(JobName=jobname)
    jobRunid = response['JobRunId']
    response = glue.get_job_run(JobName=jobname, RunId=jobRunid)
    state = response['JobRun']['JobRunState']
    
    print("state {}".format(state))
    
    # Wait for the job to stop running
    while state == 'RUNNING':
        time.sleep(180)
        response = glue.get_job_run(JobName=jobname, RunId=jobRunid)
        state = response['JobRun']['JobRunState']
        print("state {}".format(state))
    
    print("final state {}".format(state))
    
    return state

# Define the test case class for the unit test
class MyTestCase(unittest.TestCase):
    def test_data_lake(self):
        # Get resources from the CloudFormation stack
        resources_raw = getStackResources(self.STACKNAME)
        resourcesdict = {}
        
        # Create a dictionary of logical to physical resource IDs
        for resource in resources_raw:
            resourcesdict[resource['LogicalResourceId']] = resource['PhysicalResourceId']
        
        # Get the source and destination databases from the crawlers
        sourceDatabase = getcrawlerDatabase(resourcesdict['rawcrawler'])
        destinationDatabase = getcrawlerDatabase(resourcesdict['datalakecrawler'])
        
        # Delete previously created databases
        deleteDatabase(sourceDatabase)
        deleteDatabase(destinationDatabase)
        
        # Run the first crawler and check its success
        self.assertEqual(runCrawler(resourcesdict['rawcrawler']), 'SUCCEEDED')
        
        # Run the Glue job and check its success
        self.assertEqual(runJob(resourcesdict['etljob']), 'SUCCEEDED')
        
        # Run the result crawler and check its success
        self.assertEqual(runCrawler(resourcesdict['datalakecrawler']), 'SUCCEEDED')
        
        # Run an Athena query and check if the results are as expected
        response = athena.start_query_execution(QueryString='select count(*) from ' + destinationDatabase + '.us_states;', ResultConfiguration={'OutputLocation': 's3://' + resourcesdict['binariesBucket'] + '/livetestquery1/'})
        key = 'livetestquery1/' + response['QueryExecutionId'] + '.csv'
        time.sleep(5)  # Wait for the query result to be available
        s3.download_file(resourcesdict['binariesBucket'], key, 'result.csv')
        result = open('result.csv', 'r').read()
        
        self.assertEqual(result, '"_col0"\n"23884"\n')

# Main block to run the unit test
if __name__ == '__main__':
    if len(sys.argv) > 1:
        MyTestCase.STACKNAME = sys.argv.pop()
    unittest.main()

