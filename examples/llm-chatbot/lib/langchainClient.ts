import { OpenAI } from '@langchain/openai';
import { PromptTemplate } from '@langchain/core/prompts';
import { RunnableSequence } from '@langchain/core/runnables';

/**
 * Generates a personalized chat response using LangChain + OpenAI.
 *
 * The customer's unified profile (retrieved from Dozer) is injected into the system prompt
 * so the LLM can tailor its answer (e.g., recommend credit cards based on spending habits).
 *
 * @param profile - Unified customer profile object.
 * @param userMessage - The end‑user's query.
 * @returns The assistant's reply as a string.
 */
export async function generateChatResponse(profile: any, userMessage: string): Promise<string> {
  // Ensure the OpenAI key is present
  if (!process.env.OPENAI_API_KEY) {
    throw new Error('OPENAI_API_KEY is not set in environment variables.');
  }

  // Prompt template that gives the LLM the context it needs.
  const template = `
You are a helpful banking assistant. Use the following customer profile to personalize your response.
If the user asks about credit card offers, recommend the best product based on the profile data.

Customer Profile (JSON):
{profile}

User: {userMessage}
Assistant:`;

  const prompt = PromptTemplate.fromTemplate(template);

  // Build a simple chain: prompt -> OpenAI LLM
  const chain = RunnableSequence.from([
    prompt,
    new OpenAI({
      temperature: 0.7,
      openAIApiKey: process.env.OPENAI_API_KEY,
    }),
  ]);

  // Invoke the chain with formatted inputs
  const result = await chain.invoke({
    profile: JSON.stringify(profile, null, 2),
    userMessage,
  });

  // The chain returns a string; trim any leading/trailing whitespace.
  return typeof result === 'string' ? result.trim() : String(result);
}
