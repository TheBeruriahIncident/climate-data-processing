from ftplib import FTP
import os
import tarfile
from math import cos, floor
from collections import defaultdict
import sys

host = 'ftp.ncdc.noaa.gov'
ftp = FTP(host)
print("Logging into " + host)
ftp.login()
ftp.cwd('pub/data/climgrid')

print("Listing files on " + host)
filenameList = ftp.nlst()

tempDirectory = "/tmp/climate-data/"
try:
    os.mkdir(tempDirectory)
except:
    pass

fileNumber = 0


# bucket into ~500 square miles
numberOfMilesPerBucketSide = 500**(.5)

# https://gis.stackexchange.com/questions/142326/calculating-longitude-length-in-miles
milesPerDegreeLatitude = 69
milesPerDegreeLongitude = cos(40) * 69.172

degreesLatitudePerBucketSide = numberOfMilesPerBucketSide / milesPerDegreeLatitude
degreesLongitudePerBucketSide = numberOfMilesPerBucketSide / milesPerDegreeLongitude

with open("precipitation.csv", 'w') as precipitationFile, open("average-temperature.csv", 'w') as averageTemperatureFile, open("max-temperature.csv", 'w') as maxTemperatureFile, open("min-temperature.csv", 'w') as minTemperatureFile:
    for file in [precipitationFile, averageTemperatureFile, maxTemperatureFile, minTemperatureFile]:
        file.write("Year,Month,Latitude,Longitude,Value\n")

    for filename in filenameList:
        fileNumber += 1
        if fileNumber == 2:
            sys.exit()

        print("Processing {:s} ({:d}/{:d})".format(filename, fileNumber, len(filenameList)))

        tempFilePath = tempDirectory + filename
        with open(tempFilePath, 'wb') as tempFile:
            ftp.retrbinary("RETR " + filename, tempFile.write)
        tar = tarfile.open(name = tempFilePath)
        tempFolderPath = tempFilePath.strip(".tar.gz")
        try:
            os.mkdir(tempFolderPath)
        except:
            pass

        tar.extractall(path = tempFolderPath)


        for extractedFilename in os.listdir(tempFolderPath):

            year = int(extractedFilename[0:4])
            month = int(extractedFilename[4:6])

            extractedFilePath = os.path.join(tempFolderPath, extractedFilename)
            with open(extractedFilePath) as extractedFile:
                data = defaultdict(lambda: [])

                for line in extractedFile:
                    columns = list(map(float, line.split()))

                    bucket = (floor(columns[0] / degreesLatitudePerBucketSide), floor(columns[1] / degreesLongitudePerBucketSide))
                    data[bucket].append(columns)

                for bucket, bucketValue in data.items():

                    averageLatitude = sum([point[0] for point in bucketValue]) / len(bucketValue)
                    averageLongitude = sum([point[1] for point in bucketValue]) / len(bucketValue)
                    averageValue = sum([point[2] for point in bucketValue]) / len(bucketValue)

                    if ".prcp." in extractedFilename:
                        outputFile = precipitationFile
                    if ".tave." in extractedFilename:
                        outputFile = averageTemperatureFile
                    if ".tmax." in extractedFilename:
                        outputFile = maxTemperatureFile
                    if ".tmin." in extractedFilename:
                        outputFile = minTemperatureFile

                    outputFile.write("{:d},{:d},{:.6f},{:.6f},{:.2f}\n".format(year, month, averageLatitude, averageLongitude, averageValue))

os.rmdir(tempDirectory)
