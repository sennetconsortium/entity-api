class PropertyGroups:
    def __init__(self, n, t, a, d, j, l):
        """

        Parameters
        ----------
        n : List[str]
            Neo4j properties list
        t : List[str]
            Trigger (on_read_trigger) properties list
        a : List[str]
            Activity properties list
        d : List[str]
            Dependency properties list
        j : List[str]
            Schema yaml type:json_str properties list
        l : List[str]
            Schema yaml type:list properties list
        """
        self.neo4j = n
        self.trigger = t
        self.activity = a
        self.dependency = d
        self.json = j
        self.list = l