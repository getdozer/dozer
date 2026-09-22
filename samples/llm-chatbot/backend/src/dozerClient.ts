import axios from 'axios';

const DOZER_API_URL = process.env.DOZER_API_URL?.replace(/\/+$/, '');

if (!DOZER_API_URL) {
  throw new Error('DOZER_API_URL is not defined in environment variables');
}

/**
 * Fetches a unified customer profile from Dozer.
 * The query below is a placeholder – replace it with the actual Dozer query
 * that joins the required tables (customers, transactions, etc.).
 */
export async function fetchCustomerProfile(customerId: string): Promise<Record<string, any>> {
  // Example Dozer query payload – adjust according to your Dozer schema
  const payload = {
    query: `
      SELECT
        c.id as customer_id,
        c.name,
        c.email,
        SUM(t.amount) as total_spent,
        COUNT(t.id) as transaction_count,
        ARRAY_AGG(t.merchant) as merchants
      FROM customers c
      LEFT JOIN transactions t ON c.id = t.customer_id
      WHERE c.id = '${customerId}'
      GROUP BY c.id, c.name, c.email
    `
  };

  const response = await axios.post(`${DOZER_API_URL}/query`, payload, {
    headers: { 'Content-Type': 'application/json' }
  });

  // Dozer returns rows under `data` → `rows`
  const rows = response.data?.rows;
  if (!rows || rows.length === 0) {
    throw new Error(`No profile found for customer ${customerId}`);
  }

  // Return the first (and only) row
  return rows[0];
}
