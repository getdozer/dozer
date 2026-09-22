import { OpenAI } from '@langchain/openai';
import { MemoryVectorStore } from '@langchain/community/vectorstores/memory';
import { OpenAIEmbeddings } from '@langchain/openai';
import { ChatPromptTemplate, HumanMessagePromptTemplate } from '@langchain/core/prompts';
import { RunnableSequence } from '@langchain/core/runnables';
import { Document } from '@langchain/core/documents';
import { fetchCustomerProfile } from './dozerClient';

if (!process.env.OPENAI_API_KEY) {
  throw new Error('OPENAI_API_KEY is not defined in environment variables');
}

// Initialize LLM and embeddings
const llm = new OpenAI({ temperature: 0.2, modelName: 'gpt-3.5-turbo' });
const embeddings = new OpenAIEmbeddings({});

const vectorStore = new MemoryVectorStore(embeddings);

/**
 * Pre‑populate the vector store with some generic banking knowledge.
 * In a real project you would load this from a knowledge base or docs.
 */
async function seedVectorStore() {
  const docs: Document[] = [
    new Document({
      pageContent: `Our credit cards offer 1.5% cash back on groceries, 2% on travel, and 0.5% on everything else.`,
      metadata: { source: 'credit_card_faq' }
    }),
    new Document({
      pageContent: `You can increase your credit limit by requesting it through the mobile app or calling support.`,
      metadata: { source: 'credit_limit_faq' }
    }),
    new Document({
      pageContent: `If you notice a fraudulent transaction, you should report it immediately via the app or phone.`,
      metadata: { source: 'fraud_faq' }
    })
  ];

  await vectorStore.addDocuments(docs);
}

// Ensure the store is seeded once at startup
seedVectorStore().catch(console.error);

/**
 * Build a LangChain chain that:
 * 1. Retrieves relevant docs from the vector store based on the user query.
 * 2. Constructs a prompt that includes the customer's profile (hyper‑personalization)
 *    and the retrieved docs as context.
 * 3. Calls the LLM and returns the answer.
 */
export async function answerQuestion(
  customerId: string,
  userQuestion: string
): Promise<string> {
  // 1️⃣ Get customer profile from Dozer
  const profile = await fetchCustomerProfile(customerId);

  // 2️⃣ Retrieve relevant docs from vector store
  const relevantDocs = await vectorStore.similaritySearch(userQuestion, 3);

  // 3️⃣ Build the prompt
  const systemPrompt = `You are a friendly banking assistant. Use the customer's profile and the provided knowledge base to answer the question.
Customer Profile:
- Name: ${profile.name}
- Email: ${profile.email}
- Total Spent: $${profile.total_spent?.toFixed(2) ?? '0.00'}
- Transaction Count: ${profile.transaction_count ?? 0}
- Favorite Merchants: ${profile.merchants?.join(', ') ?? 'N/A'}

Knowledge Base:
${relevantDocs.map((doc, i) => `Document ${i + 1}: ${doc.pageContent}`).join('\n\n')}`;

  const prompt = ChatPromptTemplate.fromMessages([
    ['system', systemPrompt],
    new HumanMessagePromptTemplate('User: {question}')
  ]);

  const chain = RunnableSequence.from([
    {
      question: (input: { question: string }) => input.question
    },
    prompt,
    llm
  ]);

  const result = await chain.invoke({ question: userQuestion });
  // result is a LLMMessage; we just need the content string
  return typeof result === 'string' ? result : (result?.content as string);
}