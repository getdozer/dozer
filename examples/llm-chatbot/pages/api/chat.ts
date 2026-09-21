import type { NextApiRequest, NextApiResponse } from 'next';
import { getCustomerProfile } from '../../lib/dozerClient';
import { generateChatResponse } from '../../lib/langchainClient';

type DataSuccess = { response: string };
type DataError = { error: string };

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<DataSuccess | DataError>
) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  const { customerId, message } = req.body as { customerId?: string; message?: string };

  if (!customerId || !message) {
    return res.status(400).json({ error: 'Both customerId and message are required.' });
  }

  try {
    // 1️⃣ Pull unified customer profile from Dozer
    const profile = await getCustomerProfile(customerId);

    // 2️⃣ Generate a personalized answer using LangChain + OpenAI
    const answer = await generateChatResponse(profile, message);

    return res.status(200).json({ response: answer });
  } catch (err: any) {
    console.error('Chat API error:', err);
    return res.status(500).json({ error: err.message ?? 'Internal Server Error' });
  }
}
