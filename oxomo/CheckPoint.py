from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from pymongo import MongoClient
from tqdm import tqdm


class OxomoCheckPoint:
    """
    Class to handle checkpoints for Colav D Space
    """

    def __init__(self, mongodb_uri="mongodb://localhost:27017/"):
        """
        CheckPoint constructor

        Parameters:
        ----------
        mongodb_uri:str
            MongoDB connection string uri
        """
        self.client = MongoClient(mongodb_uri)
        self.registry = MetadataRegistry()
        self.registry.registerReader('oai_dc', oai_dc_reader)

    def create(self, base_url: str, mongo_db: str, mongo_collection: str, force_http_get=True):
        """
        Method to create the checkpoint, this allows to save all the ids for records and sets
        in order to know what was downloaded.
        All the checkpints are saved in the mongo collections

        Parameters:
        ----------
        base_url:str
            D-Space endpoint url
        mongo_db:str
            MongoDB database name
        mongo_collection:str
            MongoDB collection name
        force_http_get:bool
            force to use get instead post for requests
        """
        client = Client(base_url, self.registry, force_http_get=force_http_get)
        identity = client.identify()
        info = {}
        info["repository_name"] = identity.repositoryName()
        info["admin_emails"] = identity.adminEmails()
        info["base_url"] = identity.baseURL()
        info["protocol_version"] = identity.protocolVersion()
        info["earliest_datestamp"] = identity.earliestDatestamp()
        info["granularity"] = identity.granularity()

        self.client[mongo_db][f"{mongo_collection}_identity"].drop()
        self.client[mongo_db][f"{mongo_collection}_identity"].insert_one(info)

        ids = client.listIdentifiers(metadataPrefix='oai_dc')
        identifiers = []
        print("=== Getting Records ids from {}  for {}".format(
            base_url, mongo_collection))
        for i in tqdm(ids):
            identifier = {}
            identifier["_id"] = i.identifier()
            identifier["status"] = 0
            identifiers.append(identifier)
        self.client[mongo_db][f"{mongo_collection}_records_checkpoint"].drop()
        self.client[mongo_db][f"{mongo_collection}_records_checkpoint"].insert_many(
            identifiers)
        print("=== Records CheckPoint total records found = {}".format(
            len(identifiers)))

        sets_regs = client.listSets()
        sets = []

        print("=== Getting Sets ids from {}  for {}".format(
            base_url, mongo_collection))
        for s in tqdm(sets_regs):
            set_reg = {}
            set_reg["_id"] = s[0]
            set_reg["name"] = s[1]
            set_reg["status"] = 0
            sets.append(set_reg)
        self.client[mongo_db][f"{mongo_collection}_sets_checkpoint"].drop()
        self.client[mongo_db][f"{mongo_collection}_sets_checkpoint"].insert_many(
            sets)
        print("=== Sets CheckPoint total sets found = {}".format(len(sets)))

    def exists(self, mongo_db: str, mongo_collection: str):
        """
        Method to check if the checkpoints already exists.

        Parameters:
        ----------
        mongo_db:str
            MongoDB database name
        mongo_collection:str
            MongoDB collection name
        """
        ckp_rec = f"{mongo_collection}_records_checkpoint"
        ckp_set = f"{mongo_collection}_sets_checkpoint"
        collections = self.client[mongo_db].list_collection_names()
        return ckp_rec in collections and ckp_set in collections

    def drop(self, mongo_db: str, mongo_collection: str):
        """
        Method to delete all the checkpoints.

        Parameters:
        ----------
        mongo_db:str
            MongoDB database name
        mongo_collection:str
            MongoDB collection name
        """
        self.client[mongo_db][f"{mongo_collection}_identity"].drop()
        self.client[mongo_db][f"{mongo_collection}_records_checkpoint"].drop()
        self.client[mongo_db][f"{mongo_collection}_sets_checkpoint"].drop()

    def update_record(self, mongo_db: str, mongo_collection: str, keys: dict):
        """
        Method to update the status of a record in the checkpoint

        Parameters:
        ----------
        mongo_db:str
            MongoDB database name
        mongo_collection:str
            MongoDB collection name
        keys:dict
            Dictionary with _id and other required values to perform the update.
        """
        self.client[mongo_db][f"{mongo_collection}_records_checkpoint"].update_one(
            keys, {"$set": {"status": 1}})

    def update_set(self, mongo_db: str, mongo_collection: str, keys: dict):
        """
        Method to update the status of a set in the checkpoint

        Parameters:
        ----------
        mongo_db:str
            MongoDB database name
        mongo_collection:str
            MongoDB collection name
        keys:dict
            Dictionary with _id and other required values to perform the update.
        """
        self.client[mongo_db][f"{mongo_collection}_sets_checkpoint"].update_one(
            keys, {"$set": {"status": 1}})

    def get_records_regs(self, mongo_db: str, mongo_collection: str):
        """
        Function to get registers from the records ckp collection that are not downloaded

        Parameters:
        ----------
        mongo_db:str
            MongoDB database name
        mongo_collection:str
            MongoDB collection name

        Returns:
        ----------
        list
            ids of records not downloaded.
        """
        ckp_col = self.client[mongo_db][f"{mongo_collection}_records_checkpoint"]
        ckpdata = list(ckp_col.find({"status": 0}, {"status": 0}))
        return ckpdata

    def get_sets_regs(self, mongo_db: str, mongo_collection: str):
        """
        Function to get registers from the sets ckp collection that are not downloaded

        Parameters:
        ----------
        mongo_db:str
            MongoDB database name
        mongo_collection:str
            MongoDB collection name

        Returns:
        ----------
        list
            ids of sets not downloaded.
        """
        ckp_col = self.client[mongo_db][f"{mongo_collection}_sets_checkpoint"]
        ckpdata = list(ckp_col.find({"status": 0}, {"status": 0}))
        return ckpdata
