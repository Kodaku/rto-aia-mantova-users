import pandas as pd
from elasticsearch import Elasticsearch, RequestError
import json

es = Elasticsearch(
    hosts=['http://localhost:9200'],
    basic_auth=('elastic', 'e0_kX+xT1Oh_v+8pLot3')
)


def es_create_index_if_not_exists(es, index):
    try:
        es.indices.create(index=index)
    except RequestError as ex:
        print(ex)

def read_users():
    users_df = pd.read_csv("2023.6.19.ElencoAssociati.csv")
    users_df.columns = ["id", "codice_meccanografico", "qualifica", "cognome", "nome", "data_nascita", "luogo_nascita",
                        "eta", "sesso", "OT", "sezione", "cellulare", "email", "taglia", "scarpe"]
    users_df = users_df.drop(users_df.index[0])
    return users_df

def save_users_to_database(users_df):
    if not es.indices.exists(index="aia_mn_users"):
        print(f"Index users created")
        es_create_index_if_not_exists(es, "aia_mn_users")
    actions = []
    for _, row in users_df.iterrows():
        user = {"mechanographicCode": row["codice_meccanografico"], "name": row["nome"], "surname": row["cognome"]}
        action = {"index": {"_index": "aia_mn_users", "_id": row["codice_meccanografico"]}, "_op_type": "upsert"}
        doc = json.dumps(user)
        actions.append(action)
        actions.append(doc)
    res = es.bulk(index="aia_mn_users", operations=actions)
    print(res)

users_df = read_users()
save_users_to_database(users_df)