import csv
from faker import Faker
import datetime

headers = [
    # "film_id",
    "title",
    "description",
    "release_year",
    "language_id",
    "original_language_id",
    "rental_duration",
    "rental_rate",
    "length",
    "replacement_cost",
    "rating",
    "last_update",
    "special_features",
    "fulltext"
]


def datagenerate(records, headers):
    fake = Faker('en_US')

    with open("./pagila/film.csv", 'wt') as csvFile:
      # 1	ACADEMY DINOSAUR	A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies	2006	1		6	0.99	86	20.99	PG	2020-09-10 16:46:03.905795+00	{"Deleted Scenes","Behind the Scenes"}	'academi':1 'battl':15 'canadian':20 'dinosaur':2 'drama':5 'epic':4 'feminist':8 'mad':11 'must':14 'rocki':21 'scientist':12 'teacher':17
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        # writer.writeheader()
        for i in range(records):

            writer.writerow({"title": fake.name(),
                             "description": fake.paragraph(nb_sentences=3),
                             "release_year": fake.year(),
                             "language_id": 1,
                             "original_language_id": 1,
                             "rental_duration": 6,
                             "rental_rate": 0.99,
                             "length": 86,
                             "replacement_cost": 20.99,
                             "rating": "PG",
                             "last_update": "2020-09-10",
                             "special_features": None,
                             "fulltext": fake.word(),
                             })


if __name__ == '__main__':
    records = 1000000

    datagenerate(records, headers)
    print("CSV generation complete!")
