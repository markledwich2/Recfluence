from video_entities import get_entities, get_language
import unittest   # The test framework


class Test_TestIncrementDecrement(unittest.TestCase):
    def test_lowecase(self):
        lang = get_language()
        txt = [
            '''thanks i have just got to help him and jesus does one more thing if there is any little boy or girl in church this morning who got a new sled for christmas and he wants to thank jesus for the sled please please don't take our baby jesus for a ride end of chapter 29.''',
        ]

        entities = list(get_entities(lang, txt))
        res = ','.join([f'{e.type}: {e.name}' for b in entities for e in b])
        self.assertEquals('PERSON: jesus,LAW: chapter 29', res)
        #self.assertEqual(inc_dec.increment(3), 4)


if __name__ == '__main__':
    unittest.main()
