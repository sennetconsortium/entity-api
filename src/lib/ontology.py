from atlas_consortia_commons.ubkg.ubkg_sdk import UbkgSDK
from flask import current_app


# Custom accessors etc. can be added to the Ontology class
class Ontology(UbkgSDK):
    @staticmethod
    def modify_entities_cache():
        cache = current_app.ubkg.get_cache()
        entities = current_app.ubkg.entities
        key = f"VALUESET_{entities}"
        if key in cache:
            for e in cache[key]:
                if e["term"] == "Publication Entity":
                    e["term"] = "Publication"
