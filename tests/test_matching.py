import unittest

from monitor_matching import best_query_segment, normalize_text


class MatchingTests(unittest.TestCase):
    def test_expected_matches_and_rejections(self):
        cases = [
            ('iPhone 17 Pro 256GB Black', 'Куплю iPhone 17 Pro 256GB Black', True),
            ('iPhone 17 Pro 256GB Black', 'Куплю iPhone 17 Pro Max 256GB Black', False),
            ('iPhone 17 Pro Max 512GB Black eSIM', 'Xiaomi 17 Pro Max 512GB Black eSIM', False),
            ('iPhone 17 Pro 256GB Black', 'OnePlus 17 Pro 256GB Black', False),
            ('iPhone 17 Pro 256GB Black', 'Xiaomii 17 Pro 256GB Black', False),
            ('iPhone 17 Pro 256GB Black', 'Xiaomi iPhone 17 Pro 256GB Black', False),
            ('iPhone 17 Pro Max 512GB eSIM', '17 Pro Max 512 eSIM', True),
            ('iPhone 17', 'Чехол для iPhone 17', False),
            ('iPhone 17', 'iPhone 17 Case', False),
            ('Чехол для iPhone 17', 'Куплю чехол для iPhone 17', True),
            ('Чехол для iPhone 17', 'iPhone 17', False),
            ('iPhone 17 512GB Black', 'iPhone 17 256GB Black\nSamsung Galaxy S26 512GB Blue', False),
            ('iPhone 17 512GB Black', 'iPhone 17\nBlack 512GB', True),
            ('iPhone 17 256GB Black', 'iPhone 17 256GB Black\nSamsung S26 Ultra 512GB Blue', True),
            ('iPhone 17 256GB Black', 'Продам iPhone 17 256GB Black', False),
            ('iPhone 17 256GB Black', 'Продам:\niPhone 17 256GB Black', False),
            ('iPhone 17 256GB Black', 'Куплю:\niPhone 17 256GB Black', True),
            ('iPhone 17 256GB Black', 'Продам:\nSamsung S26\nКуплю:\niPhone 17 256GB Black', True),
            ('iPhone 17', 'Не нужен iPhone 17', False),
            ('iPhone 17 256GB Black', 'Куплю iPhone 17 256 Black', True),
            ('Samsung S26 Ultra 512GB Black', 'сасунг с26 ультра 512 черный', True),
            ('S26 Ultra 512GB Black', 'с26 ультра 512 черный', True),
            ('Samsung S26', 'Samsung S26 Ultra', False),
            ('Samsung S26 Ultra', 'Samsung S26', False),
            ('OnePlus 15R 12/512GB Mint Breeze', 'OnePlus 15 12/512GB Mint Breeze', False),
            ('OnePlus 15 12/512GB Mint Breeze', 'OnePlus 15R 12/512GB Mint Breeze', False),
            ('OnePlus 15R 12/512GB Mint Breeze', 'Куплю OnePlus 15R 12/512 Mint Breeze', True),
            ('Meta Ray-Ban Wayfarer F Black L Green', 'Meta Ray-Ban Wayfarer F Green L Black', False),
            ('Meta Ray-Ban Wayfarer F Black L Green', 'Meta Ray-Ban Wayfarer F Black L Green', True),
            ('ray-ban meta', 'Meta Quest 3', False),
            ('PlayStation 5 Pro', 'MacBook Pro M5', False),
            ('google pixel', 'Pixel 9', True),
            ('iPhone 17 1TB', 'iPhone 17 1024GB', True),
            ('iPhone 17 256GB Black', 'iPhone 17 512GB Black', False),
            ('OnePlus 15 12GB 512GB', 'OnePlus 15 16GB 512GB', False),
        ]
        for query, message, expected in cases:
            with self.subTest(query=query, message=message):
                score, _ = best_query_segment(query, message)
                self.assertEqual(score >= 76, expected, score)

    def test_all_mode_explicitly_allows_seller_posts(self):
        self.assertGreaterEqual(best_query_segment('iPhone 17', 'Продам iPhone 17', 'all')[0], 76)

    def test_continuation_uses_real_newline(self):
        _, segment = best_query_segment('Samsung S26 256GB Black', 'Samsung S26\nBlack 256GB')
        self.assertIn('\n', segment)
        self.assertNotIn('\\n', segment)

    def test_normalization_is_stable(self):
        for value in ('айфон17 256гб', 'iPhone 17 256GB', 'сасунг с26 ультра'):
            self.assertEqual(normalize_text(normalize_text(value)), normalize_text(value))


if __name__ == '__main__':
    unittest.main()
