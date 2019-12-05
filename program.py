import pymongo

client = pymongo.MongoClient()
reports = client['universal-dashboard']['reports']

report = {
    '_id': 201907,
    'off24': 23,
    'off18': 3,
    'setup': 12,
}
x = reports.insert_one(report)
print(x)
