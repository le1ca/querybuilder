from qbuilder import AbstractInterface, SelectQuery
import sys, os, sqlite3

class SqliteInterface (AbstractInterface):

    var = "?"
    
    def __init__(self, filename):
        self.connection = sqlite3.connect(filename)
        
    def __del__(self):
        self.connection.commit()
        self.connection.close()
    
    def fetch(self, queryString, parameters):
        cur = self.connection.execute(queryString, parameters)
        return cur.fetchall()
    
    def fetch_one(self, queryString, parameters):
        cur = self.connection.execute(queryString, parameters)
        return cur.fetchone()
    
def main():

    # delete test file if it already exists
    if os.path.exists("test.db"):
        os.remove("test.db")

    # create connection
    sql = SqliteInterface("test.db")
    
    # set up schema
    sql.connection.executescript(
        """
        CREATE TABLE users (
            uid      INTEGER PRIMARY KEY,
            username TEXT
        );
        
        CREATE TABLE notes (
            nid      INTEGER PRIMARY KEY,
            owner    INTEGER,
            content  TEXT,
            FOREIGN KEY (owner) REFERENCES users (uid)
        );
        """
    )
    sql.connection.commit()
    
    # add test data
    sql.connection.executescript(
        """
        INSERT INTO users (username)
        VALUES 
            ("alice"  ),
            ("bob"    ),
            ("charlie")
        ;
        
        INSERT INTO notes (owner, content)
        VALUES
            (1, "hello world!"),
            (1, "abcdefg"     ),
            (2, "foo bar"     ),
            (1, "qwerty"      ),
            (3, "test test"   ),
            (2, "aaaaaaaaaaa" )
        ;
        """
    )
    sql.connection.commit()
    
    # select alice's notes
    q = SelectQuery(sql)
    q.select(["username", "content"]).table("notes").table("users", "left join", "owner = uid")
    q.where("uid = ?", 1)
    q.order("nid", "asc")
    for (username, content) in q.fetch():
        print("%s: %s" % (username, content))
    
    print("\n" + "-" * 20 + "\n")
    
    # select all notes, 2 at a time
    v = None
    acc = 0
    while True:
    
        q = SelectQuery(sql)
        q.select(["nid", "username", "content"]).table("notes").table("users", "left join", "owner = uid")
        if v is not None:
            q.where("nid > ?", v, True)
        q.order("nid", "asc")
        q.limit(2)
        
        for (nid, username, content) in q.fetch():
            acc += 1
            v = nid
            print("%s: %s" % (username, content))
        
        count = q.meta()
        print("\n(seen %d so far out of %d total)\n" % (acc, count))
        if acc == count:
            break
        
    
    return 0

if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
