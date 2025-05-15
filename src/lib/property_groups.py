class PropertyGroups:
    def __init__(
        self,
        neo4j=[],
        trigger=[],
        json=[],
        list=[],
        dep=[],
        activity_neo4j=[],
        activity_trigger=[],
        activity_json=[],
        activity_list=[],
        activity_dep=[],
    ):
        """

        Parameters
        ----------
        neo4j : List[str]
            Entity neo4j properties list
        trigger : List[str]
            Entity trigger (on_read_trigger) properties list
        json : List[str]
            Entity schema yaml type:json_str properties list
        list : List[str]
            Entity schema yaml type:list properties list
        dep : List[str]
            Entity dependency properties list
        activity_neo4j : List[str]
            Activity properties list
        activity_trigger : List[str]
            Activity trigger properties list
        activity_json : List[str]
            Activity type:json_str properties list
        activity_list : List[str]
            Activity type:list properties list
        activity_dep : List[str]
            Activity dependency properties list
        """
        self.neo4j = neo4j
        self.trigger = trigger
        self.dependency = dep
        self.json = json
        self.list = list
        self.activity_neo4j = activity_neo4j
        self.activity_trigger = activity_trigger
        self.activity_json = activity_json
        self.activity_list = activity_list
        self.activity_dep = activity_dep
