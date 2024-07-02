import boto3
import subprocess
import time
import urllib.parse
import sys
import os

# Initialize AWS IAM and CodeCommit clients
client = boto3.client('iam')
codecommit = boto3.client('codecommit')

# Get the username and repository name from command line arguments
user = sys.argv[1]
reponame = sys.argv[2]

# Construct the local path for the repository
path = os.getcwd() + '/' + reponame
print(path)

# Create a service-specific credential for the user to access CodeCommit
response = client.create_service_specific_credential(UserName=user, ServiceName='codecommit.amazonaws.com')

# Get repository information from CodeCommit
ccresponse = codecommit.get_repository(repositoryName=reponame)

# Extract the necessary information from the created credential
credentialId = response["ServiceSpecificCredential"]["ServiceSpecificCredentialId"]
GitUsername = response["ServiceSpecificCredential"]["ServiceUserName"]
GitPassword = urllib.parse.quote_plus(response["ServiceSpecificCredential"]["ServicePassword"])

# Format the clone URL with the service-specific credential
url = ccresponse["repositoryMetadata"]["cloneUrlHttp"][8:]
url = "https://{0}:{1}@".format(GitUsername, GitPassword) + url

# Wait for 30 seconds to ensure the credential is ready to use
time.sleep(30)

# Clone the repository using the formatted URL
subprocess.check_call(["git", "clone", url])

# Copy YAML and Python files into the cloned repository directory
subprocess.check_call("cp -r *.yaml " + reponame, shell=True)
subprocess.check_call("cp -r *.py " + reponame, shell=True)

# Change the current working directory to the cloned repository path
os.chdir(path)

# Stage all changes for commit
subprocess.check_call("git add .", shell=True)

# Commit the changes with a message
subprocess.check_call("git commit -m initialcommit", shell=True)

# Push the changes to the remote repository
subprocess.check_call("git push", shell=True)

# Delete the service-specific credential now that it is no longer needed
response = client.delete_service_specific_credential(UserName=user, ServiceSpecificCredentialId=credentialId)
