import axios from 'axios';

const DOZER_API_URL = process.env.DOZER_API_URL?.replace(/\/+$/, '') ?? 'http://localhost:8080';

/**
 * Fetches a unified customer profile from Dozer.
 *
 * The endpoint used here mirrors the example in the Dozer blog article:
 * GET /api/v1/customers/{customerId}
 *
 * Adjust the path according to your Dozer configuration if needed.
 *
 * @param customerId - The unique identifier of the customer.
 * @returns The JSON payload representing the unified profile.
 */
export async function getCustomerProfile(customerId: string): Promise<any> {
  const url = `${DOZER_API_URL}/api/v1/customers/${encodeURIComponent(customerId)}`;
  const response = await axios.get(url, {
    timeout: 5000,
  });
  return response.data;
}