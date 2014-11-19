import subprocess
from pymongo import MongoClient

client = MongoClient()
db = client.land_register
nbr_errors = db.complete_streets.find({"profiles_processed":-1}).count()

if nbr_errors>1000:
    db.complete_streets.update({"profiles_processed":-1}, {"$set":{"profiles_processed":0}}, {"multi":"true"})
    subprocess.call("pkill python", shell=True)


