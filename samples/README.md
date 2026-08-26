# Dozer samples

Sample applications showing how to use Dozer together with other tools.

## LLM vector LangChain sample

The [`llm-vector-langchain`](llm-vector-langchain/README.md) sample implements
the use case from the [Dozer + LLM article](https://getdozer.io/blog/llm-chatbot):
Dozer sources customer profiles, transactions, and credit-card products, the
data is exposed through the Dozer API, and a LangChain application indexes it in
Chroma to answer personalized credit-card recommendation questions.