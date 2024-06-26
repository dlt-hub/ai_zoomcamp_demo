import dlt
import lancedb
import openai


openai.api_key = dlt.secrets["destination.lancedb.credentials.embedding_model_provider_api_key"]
model = dlt.secrets["destination.lancedb.embedding_model"]

query_text = "how many days of maternity leave do new mothers get?"
response = openai.embeddings.create(input=query_text,model=model)
query_embedding = response.to_dict()['data'][0]['embedding']

db = lancedb.connect("/tmp/.lancedb")
dbtable = db.open_table("notion_pages___employee_handbook")
results = dbtable.search(query=query_embedding).to_list()
print(results[0]["content"])
