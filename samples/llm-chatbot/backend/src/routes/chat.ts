import { Router, Request, Response } from 'express';
import { answerQuestion } from '../llmService';
const router = Router();
/** * POST /api/chat * Body: { customerId: string, question: string } */
router.post('/', async (req: Request, res: Response) => {
  const { customerId, question } = req.body;
  if (!customerId || !question) {
    return res.status(400).json({ error: 'customerId and question are required' });
  }
  try {
    const answer = await answerQuestion(customerId, question);
    res.json({ answer });
  } catch (err: any) {
    console.error('Chat error:', err);
    res.status(500).json({ error: err.message ?? 'Internal server error' });
  }
});
export default router;