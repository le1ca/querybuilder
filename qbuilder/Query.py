class SelectQuery (object):
    
    def __init__(self, backend):
        self._backend = backend
        self._columns = []
        self._params  = []
        self._tables  = []
        self._where   = []
        self._order   = []
        self._limit   = (None, True)
    
    def select(self, columns):
        if not isinstance(columns, list):
            columns = [columns]
        self._columns.extend(columns)
        return self
    
    def table(self, table, join=None, on=None):
        self._tables.append((table, join, on))
        return self
        
    def where(self, condition, params, nonmeta=False):
        if not isinstance(params, list):
            params = [params]
        params = [(p, nonmeta) for p in params]
        self._where.append((condition, nonmeta))
        self._params.extend(params)
        return self
    
    def limit(self, count, nonmeta=True):
        self._limit = (count, nonmeta)
        return self
    
    def order(self, column, direction=None):
        self._order.append((column, direction))
        return self
    
    def fetch(self):
        parms = [v for v,_ in self._params]
        query  = "SELECT " + ",".join(self._columns)
        query += " FROM "
        
        # non-join tables
        query += ",".join(t for t, _, __ in filter(lambda (_, j, __): j is None, self._tables))
        
        # join tables
        for t, j, o in filter(lambda (_, j, __): j is not None, self._tables):
            query += " %s %s" % (j, t)
            if o is not None:
                query += " ON %s" % (o)
                
        # where clause
        if len(self._where) > 0:
            query += " WHERE " + (" AND ".join(c for c,_ in self._where))
        
        # order-by clause
        if len(self._order) > 0:
            query += " ORDER BY " + ",".join("%s %s" % (c, o or "") for c, o in self._order)
        
        # limit clause
        if self._limit[0] is not None:
            query += " LIMIT %s" % (self._backend.var)
            parms.append(self._limit[0])
            
        return self._backend.fetch(query, parms)
    
    def meta(self):
        parms = [v for v,nm in self._params if nm == False]
        query  = "SELECT COUNT(*)"
        query += " FROM "
        
        # non-join tables
        query += ",".join(t for t, _, __ in filter(lambda (_, j, __): j is None, self._tables))
        
        # join tables
        for t, j, o in filter(lambda (_, j, __): j is not None, self._tables):
            query += " %s %s" % (j, t)
            if o is not None:
                query += " ON %s" % (o)
                
        # where clause
        where = filter(lambda (_, m): m == False, self._where)
        if len(where) > 0:
            query += " WHERE " + (" AND ".join(where))
        
        # limit clause
        if self._limit[0] is not None and self._limit[1] == False:
            query += " LIMIT %s" % (self._backend.var)
            parms.append(self._limit[0])
            
        return self._backend.fetch_one(query, parms)[0]
    
