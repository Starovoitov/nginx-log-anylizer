import unittest
import log_analyzer


class TestLogAnalyzer(unittest.TestCase):

    def test_median(self):
        self.assertEqual(log_analyzer.median([1, 1, 1, 7, 5, 8, 1, 0, 0, 0, 5, 6, 10, 100]), 3)

    def test_shift_max_time(self):
        self.assertEqual(log_analyzer.shift_max_time([6, 0.11, 0.003, 0.003, [0.003, 0.01, 0.004], 100.0, 0.012],
                                                     0.004), 0.004)

    def test_not_shift_max_time(self):
        self.assertEqual(log_analyzer.shift_max_time([6, 0.11, 0.003, 0.004, [0.003, 0.01, 0.004], 100.0, 0.012],
                                                     0.001), 0.004)

    def test_get_url_time_status(self):
        self.assertEqual(log_analyzer.get_url_time_status('1.196.116.32 -  - [29/Jun/2017:03:50:22 +0300] '
                                                          '"GET /api/v2/banner/25019354 HTTP/1.1" 200 927 "-" '
                                                          ''"Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.10.5" 
                                                          "-" '"1498697422-2190034393-4708-9752759" "dc7161be3" 0.390'
                                                          '\n'),
                         ('/api/v2/banner/25019354', 0.39, 200))

    def test_not_get_url_time_status(self):
        self.assertEqual(log_analyzer.get_url_time_status('unrecognized_line'),
                         (None, None, None))

    def test_last_log(self):
        self.assertEqual(log_analyzer.get_last_log(["testlog-20001012", "testlog-20100113", "testlog-20181225"]),
                         "testlog-20181225")

    def test_not_last_log(self):
        self.assertEqual(log_analyzer.get_last_log(["testlog-201012", "testlog-2010-01-13", "test"]),
                         None)


if __name__ == '__main__':
    unittest.main()
