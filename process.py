from collections import defaultdict
from ftplib import FTP
from math import cos, floor

import os
import sys
import tarfile

# Prepare the FTP server for reading
host = 'ftp.ncdc.noaa.gov'
ftp = FTP(host)
print("Logging into " + host)
ftp.login()
ftp.cwd('pub/data/climgrid')

# Get the list of files
print("Listing files on " + host)
filenameList = ftp.nlst()

# Prepare the temp directory to store intermediary data
tempDirectory = "/tmp/climate-data/"
try:
    os.mkdir(tempDirectory)
except:
    pass

# Bucket into ~500 square miles, so sqrt(500) on each side of the bucket
numberOfMilesPerBucketSide = 500**(.5)

# Mile to degree conversion logic from https://gis.stackexchange.com/questions/142326/calculating-longitude-length-in-miles
milesPerDegreeLatitude = 69
# This ranges from ~45 to ~60 in the continental United States; choosing the most conservative (smallest buckets) value
milesPerDegreeLongitude = 60

# The size of the buckets in degrees
degreesLatitudePerBucketSide = numberOfMilesPerBucketSide / milesPerDegreeLatitude
degreesLongitudePerBucketSide = numberOfMilesPerBucketSide / milesPerDegreeLongitude

with open("precipitation.csv", 'w') as precipitationFile, open("average-temperature.csv", 'w') as averageTemperatureFile, open("max-temperature.csv", 'w') as maxTemperatureFile, open("min-temperature.csv", 'w') as minTemperatureFile:
    # Initialize CSVs with headers
    for file in [precipitationFile, averageTemperatureFile, maxTemperatureFile, minTemperatureFile]:
        file.write("Year,Month,Latitude,Longitude,Value\n")

    fileNumber = 0
    for filename in filenameList:
        fileNumber += 1
        print("Processing {:s} ({:d}/{:d})".format(filename, fileNumber, len(filenameList)))

        # Don't process the readme
        if "readme" in filename:
            continue

        # Download file
        tempFilePath = tempDirectory + filename
        with open(tempFilePath, 'wb') as tempFile:
            ftp.retrbinary("RETR " + filename, tempFile.write)

        # Extract data from the file
        tar = tarfile.open(name = tempFilePath)
        tempFolderPath = tempFilePath.strip(".tar.gz")
        try:
            os.mkdir(tempFolderPath)
        except:
            pass
        tar.extractall(path = tempFolderPath)

        # For each of the four data types
        for extractedFilename in os.listdir(tempFolderPath):
            year = int(extractedFilename[0:4])
            month = int(extractedFilename[4:6])

            extractedFilePath = os.path.join(tempFolderPath, extractedFilename)
            with open(extractedFilePath) as extractedFile:
                # Bucket the data into a grid
                data = defaultdict(lambda: [])

                for line in extractedFile:
                    # Parse the line intro three numbers
                    columns = list(map(float, line.split()))

                    # Put the data into buckets based on a grid
                    bucket = (floor(columns[0] / degreesLatitudePerBucketSide), floor(columns[1] / degreesLongitudePerBucketSide))
                    data[bucket].append(columns)

                for bucket, bucketValue in data.items():
                    # Average the various points in each bucket
                    averageLatitude = sum([point[0] for point in bucketValue]) / len(bucketValue)
                    averageLongitude = sum([point[1] for point in bucketValue]) / len(bucketValue)
                    averageValue = sum([point[2] for point in bucketValue]) / len(bucketValue)

                    # Determine which file to write to
                    if ".prcp." in extractedFilename:
                        outputFile = precipitationFile
                    else if ".tave." in extractedFilename:
                        outputFile = averageTemperatureFile
                    else if ".tmax." in extractedFilename:
                        outputFile = maxTemperatureFile
                    else if ".tmin." in extractedFilename:
                        outputFile = minTemperatureFile
                    else
                        raise Exception("Unknown file type: " + extractedFilename)

                    # Add a row
                    outputFile.write("{:d},{:d},{:.6f},{:.6f},{:.2f}\n".format(year, month, averageLatitude, averageLongitude, averageValue))

# Clear the temp files
os.rmdir(tempDirectory)
