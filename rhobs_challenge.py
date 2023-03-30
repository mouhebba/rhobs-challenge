import json
from pymongo import MongoClient


def collection(uri):
    client = MongoClient(uri)
    database = client["rhobs"]
    collection = database["people"]
    return collection


def load(uri="localhost", datapath="data.json"):
    coll = collection(uri=uri)
    with open(datapath, "r") as fp:
        data = json.load(fp)

        for person in data:
            coll.insert_one(person)


def compter_sexe(uri="mongodb://localhost:27017/"):
    coll = collection(uri=uri)
    num_females = coll.count_documents({"sex": "F"})
    num_males = coll.count_documents({"sex": "M"})
    print(f"Nombre de femmes: {num_females}")
    print(f"Nombre d'hommes: {num_males}")


def entreprises_plus_de_n_personnes(n):
    entreprises = collection.distinct("company", {"$where": f"this.employees.length > {n}"})
    return entreprises


def pyramide_des_ages_metier(metier):
    """Renvoie la pyramide des âges pour le métier donné."""
    pipeline = [
        {"$match": {"job": metier}},
        {"$group": {"_id": {"sexe": "$sex", "age": {"$subtract": [{"$year": "$$NOW"}, {"$year": {"$dateFromString": {"dateString": "$birthdate"}}}] / 10 * 10}}, "count": {"$sum": 1}}},
        {"$group": {"_id": "$_id.age", "hommes": {"$sum": {"$cond": [{"$eq": ["$_id.sexe", "M"]}, "$count", 0]}}, "femmes": {"$sum": {"$cond": [{"$eq": ["$_id.sexe", "F"]}, "$count", 0]}}}},
        {"$sort": {"_id": 1}}
    ]
    resultats = list(collection.aggregate(pipeline))
    return resultats


# Charger la BD sur localhost:27017 à partir de "data.json.codechallenge.janv22.RHOBS.json"
load(uri="mongodb://localhost:27017/", datapath="data.json.codechallenge.janv22.RHOBS.json")

# Compter les femmes et les hommes
compter_sexe()

# Entreprises plus de 5 personees
print(entreprises_plus_de_n_personnes(5))

# Pyramide des ages pour le metier 'psychanalyste'
print(pyramide_des_ages_metier('psychanalyste'))
