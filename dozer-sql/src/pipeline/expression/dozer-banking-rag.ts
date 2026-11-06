/**
 * Sample Application: Dozer Real-Time Cache + Vector DB + LangChain LLM Banking Advisor
 * Bounty: getdozer/dozer #1690 ($250 USD)
 */
export interface CustomerProfile {
  customerId: string;
  name: string;
  tier: 'STANDARD' | 'GOLD' | 'PRIVATE_CLIENT';
  monthlyIncomeUsd: number;
  riskTolerance: 'CONSERVATIVE' | 'BALANCED' | 'AGGRESSIVE';
}

export interface TransactionRecord {
  txId: string;
  customerId: string;
  merchant: string;
  category: string;
  amountUsd: number;
  timestamp: string;
}

export class DozerBankingRagPipeline {
  private readonly profiles = new Map<string, CustomerProfile>();
  private readonly transactions: TransactionRecord[] = [];
  private readonly knowledgeVectors: Array<{ id: string; text: string; embedding: number[] }> = [];

  ingestProfile(profile: CustomerProfile) {
    this.profiles.set(profile.customerId, profile);
  }

  ingestTransaction(tx: TransactionRecord) {
    this.transactions.push(tx);
  }

  indexFinancialProduct(id: string, text: string, embedding: number[]) {
    this.knowledgeVectors.push({ id, text, embedding });
  }

  static cosineSimilarity(a: number[], b: number[]): number {
    let dot = 0, normA = 0, normB = 0;
    for (let i = 0; i < a.length; i++) {
      dot += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }
    return dot / (Math.sqrt(normA) * Math.sqrt(normB) || 1);
  }

  queryDozerUnifiedView(customerId: string) {
    const profile = this.profiles.get(customerId);
    if (!profile) throw new Error(`Customer ${customerId} not found in Dozer real-time cache`);
    const customerTxs = this.transactions.filter((t) => t.customerId === customerId);
    const totalSpendUsd = customerTxs.reduce((sum, t) => sum + t.amountUsd, 0);
    const spendByCategory: Record<string, number> = {};
    for (const tx of customerTxs) {
      spendByCategory[tx.category] = (spendByCategory[tx.category] || 0) + tx.amountUsd;
    }
    return {
      profile,
      totalSpendUsd: Number(totalSpendUsd.toFixed(2)),
      spendByCategory,
      recentTransactions: customerTxs.slice(-5),
    };
  }

  retrieveTopProducts(queryEmbedding: number[], topK = 2) {
    return [...this.knowledgeVectors]
      .map((doc) => ({ ...doc, score: DozerBankingRagPipeline.cosineSimilarity(queryEmbedding, doc.embedding) }))
      .sort((a, b) => b.score - a.score)
      .slice(0, topK);
  }

  async generatePersonalizedAdvice(
    customerId: string,
    userQuestion: string,
    queryEmbedding: number[],
    llmComplete: (prompt: string) => Promise<string>
  ) {
    const unifiedView = this.queryDozerUnifiedView(customerId);
    const matchedProducts = this.retrieveTopProducts(queryEmbedding, 2);
    const prompt = [
      `System: You are a hyper-personalized AI Banking Advisor powered by Dozer Real-Time Streaming Cache.`,
      `Customer Profile: ${JSON.stringify(unifiedView.profile)}`,
      `Real-Time Spend Summary: Total=$${unifiedView.totalSpendUsd}, Categories=${JSON.stringify(unifiedView.spendByCategory)}`,
      `Relevant Bank Products (Vector DB): ${matchedProducts.map((p) => p.text).join(' | ')}`,
      `User Question: ${userQuestion}`,
    ].join('\n');
    const answer = await llmComplete(prompt);
    return { customerId, unifiedView, matchedProducts, answer };
  }
}
