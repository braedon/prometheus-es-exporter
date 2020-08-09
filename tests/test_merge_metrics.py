import unittest

from prometheus_es_exporter.metrics import merge_metric_dicts
from tests.utils import convert_metric_dict


class Test(unittest.TestCase):
    maxDiff = None

    def test_new_metric(self):
        old_dict = {
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1}),
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 1,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict))
        self.assertEqual(expected, result)

    def test_updated_metric(self):
        old_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1}),
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 2}),
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 2,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict))
        self.assertEqual(expected, result)

    def test_missing_metric_preserve(self):
        old_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1}),
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 1,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict, zero_missing=False))
        self.assertEqual(expected, result)

    def test_missing_metric_zero(self):
        old_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1}),
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 0,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict, zero_missing=True))
        self.assertEqual(expected, result)

    def test_new_label_keys(self):
        old_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1}),
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 2,
                                                       ('c', 'd'): 1}),
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 2,
            'foo{bar="c",baz="d"}': 1,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict))
        self.assertEqual(expected, result)

    def test_updated_label_keys(self):
        old_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1,
                                                       ('c', 'd'): 1}),
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 2,
                                                       ('c', 'd'): 2}),
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 2,
            'foo{bar="c",baz="d"}': 2,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict))
        self.assertEqual(expected, result)

    def test_missing_label_keys_preserve(self):
        old_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1,
                                                       ('c', 'd'): 1}),
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 2}),
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 2,
            'foo{bar="c",baz="d"}': 1,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict, zero_missing=False))
        self.assertEqual(expected, result)

    def test_missing_label_keys_zero(self):
        old_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 1,
                                                       ('c', 'd'): 1}),
            'other': ('other docstring', (), {(): 1}),
        }
        new_dict = {
            'foo': ('test docstring', ('bar', 'baz'), {('a', 'b'): 2}),
            'other': ('other docstring', (), {(): 2}),
        }

        expected = {
            'foo{bar="a",baz="b"}': 2,
            'foo{bar="c",baz="d"}': 0,
            'other': 2,
        }
        result = convert_metric_dict(merge_metric_dicts(old_dict, new_dict, zero_missing=True))
        self.assertEqual(expected, result)


if __name__ == '__main__':
    unittest.main()
