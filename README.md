# Capstone_G2_Message_System

to set up your local dev envir to set up google cloud follow instructions in this link. 
https://docs.cloud.google.com/docs/authentication/set-up-adc-local-dev-environment
after gccloud is installed and authenicated run this
c:\>gcloud auth application-default login
and it will spit out the location of a json file with credentials
optional if using windows: run 
c:\>setx GOOGLE_APPLICATION_CREDENTIALS C:\Users\avery\AppData\Roaming\gcloud\application_default_credentials.json