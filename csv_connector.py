import csv
import json
import logging
import time
import sys
import os
import glob
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch import helpers


class CSVConnector:
    FILENAME = "data/alerts_table_hullkeeper.csv"
    ONLY_ONE_DATAROW = True

    def __init__(self):
        self.elasticindexer = ElasticIndexer()
        self.log = logging.getLogger("csv_connector")

    def run(self):
        self.log.info(f"Reading file: {self.FILENAME}")
        self.read_cvs_file(self.FILENAME)

    def read_cvs_file(self, filename):
        with open(filename) as csv_file:
            reader = csv.DictReader(csv_file)
            for line in reader:
                self.create_doc(line)

        self.elasticindexer.index_documents()

    def create_doc(self, doc_row):
        # Splits every row with data into separate documents
        es_doc = {}

        # Create geofield
        if "Latitude" in doc_row and "Longitude" in doc_row:
            es_doc["location"] = {
                "lat": doc_row["Latitude"],
                "lon": doc_row["Longitude"],
            }

        for key in doc_row:
            if key == "ETA":
                es_doc[self._trim(key)] = self._convert_date(doc_row[key])
            elif key == "Final Date":
                es_doc[self._trim(key)] = self._convert_date(doc_row[key])
            elif key == "Initial Date":
                es_doc[self._trim(key)] = self._convert_date(doc_row[key])
            else:
                es_doc[self._trim(key)] = doc_row[key]

        self.elasticindexer.add_doc(es_doc)

    def _convert_date(self, date_string):
        # 2017-11-11 17:00:00
        if not date_string:
            return None

        timestamp = datetime.strptime(date_string, "%Y-%m-%d %H:%M:%S")
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.00Z")

    def _trim(self, term):
        # Returns a safe string for field names in elastic
        return term.lower().replace(" ", "_")


class ElasticIndexer:
    BATCHSIZE = 1000
    INDEX = "jotundata"
    TYPE = "_doc"
    ELASTIC_URL = "localhost:9200"
    USER = "user"
    PASS = "pass"

    def __init__(self):
        self.es = Elasticsearch(
            hosts=self.ELASTIC_URL,
            raise_on_error=False,
            http_auth=(self.USER, self.PASS),
            use_ssl=False,
            verify_certs=True,
        )

        self.actions = []
        self.log = logging.getLogger("elastic_indexer")
        self.total_documents = 0
        self.start = time.time()
        self.add_template()

    def add_template(self):
        template = {
            "index_patterns": [f"{self.INDEX}*"],
            "mappings": {"properties": {"location": {"type": "geo_point"}}},
        }

        self.es.indices.put_template(name=self.INDEX, body=json.dumps(template))

    def add_doc(self, doc):
        action = {}

        action["_op_type"] = "index"
        action["_index"] = self.INDEX
        action["_type"] = self.TYPE
        # action["_id"] =
        action["_source"] = doc

        self.log.debug(f"Appending doc: {action}")
        self.actions.append(action)

        if len(self.actions) >= self.BATCHSIZE:
            self.index_documents()

    def index_documents(self):
        if len(self.actions) < 1:
            return

        sucess, respons = helpers.bulk(self.es, self.actions, stats_only=True)

        if respons is 0:
            self.total_documents = self.total_documents + sucess
            self.log.info(
                f"Indexed successful {sucess} documents - Total documents indexed {self.total_documents} - {sucess/(time.time()-self.start):.2f} docs/s"
            )
        else:
            self.log.error(
                f"Indexing error with {respons} documents, successful indexed {sucess} documents"
            )

        self.start = time.time()
        self.actions = []


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    log = logging.getLogger("elastic_connector")

    connector = CSVConnector()
    connector.run()