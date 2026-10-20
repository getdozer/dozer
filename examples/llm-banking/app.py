#!/usr/bin/env python3
"""
CLI Demo Application for Dozer + LLM + Vector Database Banking Sample.
"""

import sys
import os
import json
from recommend import HyperPersonalizedRecommender

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "data")
    recommender = HyperPersonalizedRecommender(data_dir=data_dir)

    target_customer = sys.argv[1] if len(sys.argv) > 1 else "CUST001"
    print(f"[*] Processing Hyper-Personalized Recommendation for: {target_customer}")
    
    result = recommender.generate_recommendation(target_customer)
    print("\n--- Recommendation Result ---")
    print(json.dumps(result, indent=2))
    print("\nChatbot Response:")
    print(result["rationale"])

if __name__ == "__main__":
    main()
