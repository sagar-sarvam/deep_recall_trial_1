from groq import Groq

client = Groq(api_key="")

MESSAGE_HISTORY = []  # only stores the last 3 messages

def chat_with_llm(user_query: str, retrieval_content: str) -> str:
    global MESSAGE_HISTORY

    system_prompt = (
        "You are a helpful assistant. Use the given retrieval content to answer "
        "the user's question accurately. If the information is not available in"
        "the data given to you or you are unsure about it, only say you don't know"
        "unless that information was given by the user themselves."
    )

    MESSAGE_HISTORY.append({"role": "user", "content": f"Context:\n{retrieval_content}\n\nUser query: {user_query}"})

    MESSAGE_HISTORY = MESSAGE_HISTORY[-3:]

    messages = [{"role": "system", "content": system_prompt}] + MESSAGE_HISTORY

    response = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        messages=messages,
        temperature=0.2,
        max_tokens=1000,
    )

    model_reply = response.choices[0].message.content.strip()

    MESSAGE_HISTORY.append({"role": "assistant", "content": model_reply})
    MESSAGE_HISTORY = MESSAGE_HISTORY[-6:]

    return model_reply

if __name__ == "__main__":
    retrieval_content = (
        "The Chernobyl disaster was a catastrophic nuclear accident that occurred on April 26, 1986, "
        "at the Chernobyl Nuclear Power Plant in the Ukrainian SSR. It is considered the worst nuclear "
        "disaster in history, both in terms of cost and casualties. The accident released large quantities "
        "of radioactive particles into the atmosphere, which spread over much of Western USSR and Europe."
    )

    print("Context loaded: Chernobyl Disaster Information")
    print("You can now ask questions. Type 'exit' or 'quit' to stop.\n")

    while True:
        user_query = input("Enter your question about the Chernobyl disaster: ").strip()
        if user_query.lower() in ("exit", "quit"):
            print("exiting")
            break

        answer = chat_with_llm(user_query, retrieval_content)
        print(f"\nAnswer: {answer}\n{'-'*60}\n")

