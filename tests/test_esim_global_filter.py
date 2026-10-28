"""The eSIM-only monitor must not silently accept a global SIM variant."""
import unittest

from monitor_matching import best_query_segment


class EsimGlobalFilterTests(unittest.TestCase):
    base = 'iPhone 17 Pro Max 512GB Cosmic Orange'

    def matches(self, query_sim, message_sim):
        query = f'{self.base} {query_sim}'
        message = f'{self.base} {message_sim}'
        return best_query_segment(query, message)[0] >= 76

    def test_only_esim_rejects_global_or_physical_sim_without_proof(self):
        queries = ('eSIM', 'только eSIM', 'eSIM only', 'онли есим', 'esim онли')
        messages = (
            ('Global eSIM', False),
            ('eSIM global', False),
            ('глобал есим', False),
            ('глоб есим', False),
            ('GLOBAL 1Sim+Esim', False),
            ('1Sim+Esim', False),
            ('SIM+eSIM', False),
            ('1 SIM', False),
            ('2 SIM', False),
            ('Global only', False),
            ('Global', False),
            ('🇺🇸', False),
            ('🇪🇺', False),
            ('eSIM only 1sim', False),
            ('global eSIM only 1 sim', False),
        )
        for query in queries:
            for message, expected in messages:
                with self.subTest(query=query, message=message):
                    self.assertEqual(self.matches(query, message), expected)

    def test_esim_and_explicit_only_forms_are_equivalent(self):
        queries = ('eSIM', 'только eSIM', 'eSIM only', 'онли есим', 'esim онли')
        messages = ('eSIM', 'eSIM only', 'only eSIM', 'только eSIM', 'онли есим', '2 eSIM')
        for query in queries:
            for message in messages:
                with self.subTest(query=query, message=message):
                    self.assertTrue(self.matches(query, message))

    def test_explicit_only_can_override_ambiguous_global_label_but_not_a_physical_sim(self):
        for query in ('eSIM', 'только eSIM', 'eSIM only'):
            with self.subTest(query=query):
                self.assertTrue(self.matches(query, 'Global eSIM only'))
                self.assertFalse(self.matches(query, 'Global eSIM only + 1 Sim'))

    def test_explicit_esim_count_requires_same_count(self):
        self.assertTrue(self.matches('2 eSIM', '2 eSIM'))
        self.assertTrue(self.matches('2 eSIM', 'две eSIM'))
        self.assertFalse(self.matches('2 eSIM', 'eSIM'))
        self.assertFalse(self.matches('2 eSIM', '1 eSIM'))
        self.assertTrue(self.matches('eSIM', '2 eSIM'))

    def test_no_physical_slot_is_not_assumed_from_global(self):
        for query in ('без сим', 'без физической сим', 'no sim'):
            with self.subTest(query=query):
                self.assertFalse(self.matches(query, 'Global eSIM'))

    def test_explicit_global_request_is_not_treated_as_esim_only(self):
        self.assertTrue(self.matches('Global eSIM', 'Global eSIM'))
        self.assertTrue(self.matches('Global eSIM', 'Global 1Sim+eSIM'))
        self.assertFalse(self.matches('Global eSIM', 'eSIM only'))


if __name__ == '__main__':
    unittest.main()
