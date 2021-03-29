from datetime import datetime, timezone, tzinfo
from jsonl import to_jsonl
from video_entities import DbVideoEntity, VideoEntity, clean_text, get_entities, get_language, videos_to_entities
import unittest   # The test framework


class Test_VideoEntities(unittest.TestCase):
    def test_lowecase(self):
        txt = [
            '''thanks i have just got to help him and jesus does one more thing if there is any little boy or girl in church this morning who got a new sled for christmas and he wants to thank jesus for the sled please please don't take our baby jesus for a ride end of chapter 29.''',
        ]

        entities = list(get_entities(get_language(), txt))
        res = ','.join([f'{e.type}: {e.name}' for b in entities for e in b])
        self.assertEquals('TIME: this morning,DATE: christmas,PERSON: jesus,LAW: chapter 29', res)

    def test_video_entity_regression(self):
        dt = datetime(2020, 12, 1, 0, 0, 0, 0, timezone.utc)
        entities = videos_to_entities([
            DbVideoEntity('v1', 'VideoTitle', None, None, dt, dt),
            DbVideoEntity('v2', '''How does Bill Gates get so much conspiracy attention''',
                          'description goes here', '[{"caption":"UPPERCASE CAPTIONS SHOULD BE\\nCLEANED OF NEWLINES\\nSWEET BABY JESUS, MARY AND JOSEPH", "offset":120}]', dt, dt)
        ])

        json = to_jsonl([VideoEntity(e.videoId, e.part, e.offset, e.entities) for e in entities])
        self.assertEquals(json, '{"videoId": "v1", "part": "title", "offset": null, "entities": [], "videoUpdated": null, "captionUpdated": null, "updated": null}\n{"videoId": "v2", "part": "title", "offset": null, "entities": [{"name": "Bill Gates", "type": "PERSON", "start_char": 9, "end_char": 19}], "videoUpdated": null, "captionUpdated": null, "updated": null}\n{"videoId": "v1", "part": "description", "offset": null, "entities": [], "videoUpdated": null, "captionUpdated": null, "updated": null}\n{"videoId": "v2", "part": "description", "offset": null, "entities": [], "videoUpdated": null, "captionUpdated": null, "updated": null}\n{"videoId": "v2", "part": "caption", "offset": 120, "entities": [{"name": "mary", "type": "PERSON", "start_char": 67, "end_char": 71}, {"name": "joseph", "type": "PERSON", "start_char": 76, "end_char": 82}], "videoUpdated": null, "captionUpdated": null, "updated": null}')

    def test_clean_text_edge_cases(self):
        clean_text(' one two three\n four   ')  # just test it doesn't break with delemeters at the end
        self.assertEquals('one two three four five six', clean_text('ONE TWO THREE FOUR FIVE SIX'))


if __name__ == '__main__':
    unittest.main()
