import { useState } from 'react';
import axios from 'axios';

export default function Home() {
  const [customerId, setCustomerId] = useState('');
  const [message, setMessage] = useState('');
  const [response, setResponse] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sendMessage = async () => {
    setLoading(true);
    setError(null);
    setResponse(null);
    try {
      const res = await axios.post('/api/chat', {
        customerId,
        message,
      });
      setResponse(res.data.response);
    } catch (e: any) {
      setError(e?.response?.data?.error ?? e.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <main style={{ padding: '2rem', fontFamily: 'sans-serif' }}>
      <h1>Dozer‑Powered Banking Chatbot</h1>

      <div style={{ marginBottom: '1rem' }}>
        <label>
          Customer ID:{' '}
          <input
            type="text"
            value={customerId}
            onChange={(e) => setCustomerId(e.target.value)}
            placeholder="e.g. 12345"
          />
        </label>
      </div>

      <div style={{ marginBottom: '1rem' }}>
        <label>
          Your Message:{' '}
          <textarea
            rows={4}
            cols={50}
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            placeholder="Ask about credit card offers..."
          />
        </label>
      </div>

      <button onClick={sendMessage} disabled={loading || !customerId || !message}>
        {loading ? 'Thinking...' : 'Send'}
      </button>

      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {response && (
        <div style={{ marginTop: '2rem', padding: '1rem', border: '1px solid #ddd' }}>
          <h2>Assistant</h2>
          <p>{response}</p>
        </div>
      )}
    </main>
  );
}
