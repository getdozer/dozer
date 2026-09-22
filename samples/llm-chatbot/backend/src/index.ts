import express, { Request, Response } from 'express';
import dotenv from 'dotenv';
import chatRouter from './routes/chat';

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

app.use(express.json());

// Simple health check
app.get('/health', (_req: Request, res: Response) => {
  res.json({ status: 'ok' });
});

app.use('/api/chat', chatRouter);

app.listen(port, () => {
  console.log(`🚀 Backend listening on http://localhost:${port}`);
});