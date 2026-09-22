import { useState } from 'react';
import axios from 'axios';
import styles from '../styles/Home.module.css';

export default function Home() {
  const [customerId, setCustomerId] = useState('cust_123');
  const [question, setQuestion] = useState('');
  const [answer, setAnswer] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setAnswer(null);
    try {
      const resp = await axios.post('/api/chat', {
        customerId,
        question
      });
      setAnswer(resp.data.answer);
    } catch (err: any) {
      setError(err.response?.data?.error ?? err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className={styles.main}>
      <h1 className={styles.title}>Dozer‑Powered Banking Chatbot</h1>

      <form onSubmit={handleSubmit} className={styles.form}>
        <label>
          Customer ID:
          <input
            type="text"
            value={customerId}
            onChange={(e) => setCustomerId(e.target.value)}
            required
            className={styles.input}
          />
        </label>

        <label>
          Question:
          <textarea
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            required
            rows={4}
            className={styles.textarea}
          />
        </label>

        <button type="submit" disabled={loading} className={styles.button}>
          {loading ? 'Thinking...' : 'Ask'}
        </button>
      </form>

      {error && <p className={styles.error}>Error: {error}</p>}

      {answer && (
        <div className={styles.answerBox}>
          <h2>Answer</h2>
          <p>{answer}</p>
        </div>
      )}
    </main>
  );
}