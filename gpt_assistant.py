import dlt
import lancedb
import openai
import json


openai.api_key = dlt.secrets["destination.lancedb.credentials.embedding_model_provider_api_key"]
model = dlt.secrets["destination.lancedb.embedding_model"]

def embed_question(question):

    response = openai.embeddings.create(input=question,model=model)
    embedded_question = response.to_dict()['data'][0]['embedding']

    return embedded_question

def retrieve_context_from_lancedb(db, embedded_question, top_k=3):

    dbtable = db.open_table("notion_pages___employee_handbook")
    query_results = dbtable.search(query=embedded_question).to_list()
    context = "\n".join([result["content"] for result in query_results[:top_k]])
    
    return context

def create_gpt_input(question,context):
    prompt = f'Question: "{question}"; Context:"{context}"'
    messages = [
        {"role": "system", "content": "You are a helpful assistant that helps users understand policies inside a company's employee handbook. The user will first ask you a question and then provide you relevant paragraphs from the handbook as context. Please answer the question based on the provided context. For any details missing in the paragraph, encourage the employee to contact the HR for that information. Please keep the responses conversational."},
        {"role": "user", "content": prompt}
    ]

    return messages

def main():

    db = lancedb.connect("/tmp/.lancedb")

    while True:
        question = input("You: ")
        embedded_question = embed_question(question)
        context = retrieve_context_from_lancedb(db,embedded_question)
        gpt_input = create_gpt_input(question,context)
        response = openai.chat.completions.create(
            model="gpt-4o",
            messages=gpt_input
        )
        print(f"Assistant: {response.choices[0].message.content}")


if __name__ == '__main__':
    main()
