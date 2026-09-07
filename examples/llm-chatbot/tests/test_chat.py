import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bank import get_profile, load_bank
from chat import BagOfWords, index_cards, retrieve


class ChatTest(unittest.TestCase):
    def test_travel_profile_hits_travel_card(self):
        bank = load_bank()
        maya = get_profile("c1", bank)
        docs = retrieve(index_cards(bank["cards"]), maya, k=2)
        names = [d.metadata["name"] for d in docs]
        self.assertTrue(
            any("Travel" in n or "Drift" in n for n in names),
            names,
        )

    def test_student_hits_campus_card(self):
        bank = load_bank()
        ana = get_profile("c5", bank)
        docs = retrieve(index_cards(bank["cards"]), ana, k=2)
        names = [d.metadata["name"] for d in docs]
        self.assertIn("Campus Starter", names)

    def test_bag_of_words_is_stable(self):
        e = BagOfWords()
        e.embed_documents(["travel lounge dining", "groceries cashback"])
        q = e.embed_query("travel lounge")
        self.assertEqual(len(q), len(e.vocab))
        self.assertGreater(sum(q), 0)


if __name__ == "__main__":
    unittest.main()
