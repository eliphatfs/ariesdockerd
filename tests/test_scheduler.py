import unittest
from ariesdockerd.error import AriesError
from ariesdockerd.scheduling import schedule


class TestScheduler(unittest.TestCase):

    def test_schedule(self):
        self.assertListEqual(schedule({'A': [0, 1, 2], 'B': [7]}, 1, 2), [('A', [0, 1])])
        self.assertListEqual(schedule({'A': [0, 1, 2], 'B': [5, 6]}, 1, 2), [('B', [5, 6])])
        self.assertListEqual(
            sorted(schedule({'A': [0, 1, 2], 'B': [5, 6]}, 2, 2)),
            sorted([('A', [0, 1]), ('B', [5, 6])])
        )
        self.assertListEqual(
            sorted(schedule({'A': [0, 1, 2, 3], 'B': [5, 6, 7, 8], 'C': [0, 1, 2, 3]}, 3, 4)),
            sorted({'A': [0, 1, 2, 3], 'B': [5, 6, 7, 8], 'C': [0, 1, 2, 3]}.items())
        )
        self.assertRaises(AriesError, schedule, {'A': [0], 'B': [5, 6, 7]}, 1, 4)
        self.assertRaises(AriesError, schedule, {'A': [0], 'B': [5, 6, 7]}, 2, 2)
        self.assertRaises(AriesError, schedule, {'A': [0], 'B': [5, 6, 7]}, 5, 1)
        self.assertListEqual(
            (schedule({'A': [0, 1, 2], 'B': [5, 6]}, 3, 0)),
            ([('A', []), ('B', []), ('A', [])])
        )
