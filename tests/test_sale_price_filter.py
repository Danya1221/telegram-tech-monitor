"""Price-shaped sale announcements must not masquerade as buyer requests."""
import unittest

from monitor_matching import best_query_segment


class SalePriceFilterTests(unittest.TestCase):
    iphone_query = 'iPhone 17 Pro Max SIM 512Gb Cosmic Orange'
    iphone = 'iPhone 17 Pro Max 512 Cosmic Orange (1 Sim) 🇪🇺'

    def matched(self, text, *, query=None, mode='buy'):
        return best_query_segment(query or self.iphone_query, text, mode)[0] >= 76

    def test_screenshot_and_price_notations_are_not_buy_requests(self):
        endings = (
            ' - 124000', ' — 124000', ' – 124000',
            ' - 124 000', ' — 124\u00a0000', ' — 124\u202f000',
            ' - 124000 ₽', ' - 124 000 руб', ' - 124 000 рублей',
            ' — 124000 RUB', ' 124000 ₽', ' 124 000 руб',
            ' — 124.000', ' цена: 124000', ' - 124000 торг',
            ' - 124000 р.', ' - 124000 🇷🇺', ' - 124000 Москва',
            ' - 124000 🔥', ' - 124000 📍 Москва', ' - 124000 от 1 шт',
            ' - 124000 ₽ (розница)',
        )
        for ending in endings:
            with self.subTest(ending=ending):
                self.assertFalse(self.matched(self.iphone + ending))

    def test_all_mode_explicitly_allows_the_same_priced_offers(self):
        for ending in (' - 124000', ' — 124 000 ₽', ' 124000 ₽'):
            with self.subTest(ending=ending):
                self.assertTrue(self.matched(self.iphone + ending, mode='all'))

    def test_explicit_purchase_with_budget_or_price_still_matches(self):
        messages = (
            'Куплю ' + self.iphone + ' до 124000',
            'Куплю ' + self.iphone + ' — 124000',
            'Нужен ' + self.iphone + ' за 124 000 ₽',
            'Куплю:\n' + self.iphone + ' — 124 000 ₽',
        )
        for text in messages:
            with self.subTest(text=text):
                self.assertTrue(self.matched(text))

    def test_purchasing_header_is_kept_for_queued_message_revalidation(self):
        text = 'Куплю:\n' + self.iphone + ' — 124 000 ₽'
        score, segment = best_query_segment(self.iphone_query, text, 'buy')
        self.assertGreaterEqual(score, 76)
        self.assertGreaterEqual(best_query_segment(self.iphone_query, segment, 'buy')[0], 76)
        self.assertIn('Куплю', segment)

    def test_buy_header_survives_product_and_attribute_continuation(self):
        query = 'iPhone 17 512 Black'
        message = 'Куплю:\niPhone 17 Black — 124000\n512GB'
        score, segment = best_query_segment(query, message, 'buy')
        self.assertGreaterEqual(score, 76)
        self.assertGreaterEqual(best_query_segment(query, segment, 'buy')[0], 76)
        self.assertIn('Куплю', segment)

    def test_unlabelled_buyer_budget_does_not_become_a_sale(self):
        for suffix in (' до 124000 ₽', ' бюджет 124000 ₽', ' не дороже 124000 руб'):
            with self.subTest(suffix=suffix):
                self.assertTrue(self.matched(self.iphone + suffix))

    def test_thousands_abbreviations_are_not_purchase_requests(self):
        for ending in (' - 124к', ' — 124 тыс', ' - 124k'):
            with self.subTest(ending=ending):
                self.assertFalse(self.matched(self.iphone + ending))

    def test_short_request_sim_memory_and_supplier_sku_are_not_a_price(self):
        messages = (
            self.iphone,
            'iPhone 17 Pro Max 512 Cosmic Orange 1 SIM',
            'iPhone 17 Pro Max 512 Cosmic Orange (1 Sim) CPH2749',
            'iPhone 17 Pro Max 512 Cosmic Orange (1 Sim) SM-S731B/DS',
        )
        for text in messages:
            with self.subTest(text=text):
                self.assertTrue(self.matched(text))

    def test_mixed_post_can_select_the_buy_row_but_not_the_sale_row(self):
        samsung_query = 'Samsung Galaxy S25 FE 256 Black'
        text = ('Продам:\n' + self.iphone + ' — 124000\n'
                'Куплю:\nSamsung Galaxy S25 FE 256 Black')
        self.assertFalse(self.matched(text))
        self.assertTrue(self.matched(text, query=samsung_query))

    def test_unmarked_sale_before_an_explicit_buy_does_not_get_selected(self):
        text = self.iphone + ' — 124000\nКуплю Samsung Galaxy S25 FE 256 Black'
        self.assertFalse(self.matched(text))
        self.assertTrue(self.matched(text, query='Samsung Galaxy S25 FE 256 Black'))


if __name__ == '__main__':
    unittest.main()
