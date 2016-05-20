class AbstractInterface (object):
    
    def fetch(queryString, parameters):
        raise NotImplementedError()
    
    def fetch_one(queryString, parameters):
        raise NotImplementedError()
