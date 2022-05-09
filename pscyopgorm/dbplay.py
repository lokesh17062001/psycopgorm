from tokenize import String
from venv import create
from psycopg2 import connect
import psycopg2.extras

class DBPlay:
    def __init__(self):
        self.postgres_config = {
            "host":"localhost",
            "port":"5432",
            "dbname":"testdatabase",
            "user":"lokesh",
            "password":"newpassword",
        }
        
    def create_table(self,a:String,b:int):
        li=[];
        c='('
        types=[' INT',' VARCHAR(25)']
        for i in range(b):
            field = input('Enter Field: ')
            if field not in li:
                li.append(field)
                c += field
                c += types[int(input('Assign type "1" as int "2" as varchar: '))-1]+','
            else:
                li.append(field+'1')
                c += field+'1'
                c += types[int(input('Assign type "1" as int "2" as varchar: '))-1]+','    
                print('Field created with "'+field+'1"')       
        c = c[:-1]+');'
        
            
        with connect(**self.postgres_config) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                
                cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (a,))
                if cur.fetchone()[0]:
                    print("Table already exists")
                else:
                    print("Table Created")
                cur.execute('CREATE TABLE IF NOT EXISTS '+a+c)
        print('CREATE TABLE IF NOT EXISTS '+a+c)
        
        
    def drop_table(self, a:String):
        with connect(**self.postgres_config)as conn:
            conn.autocommit = True
            with conn.cursor() as curr:
                curr.execute("select exists(select * from information_schema.tables where table_name=%s)", (a,))
                
                if curr.fetchone()[0] == True:
                    print("Table Dropped")
                else:
                    print("Sorry!!! table doesn't exist")
                curr.execute("DROP TABLE IF EXISTS "+a+";")
                
    def rename_table(self,a:String,b:String):
        with connect(**self.postgres_config)as conn:
            conn.autocommit = True
            with conn.cursor() as curr:
                curr.execute("select exists(select * from information_schema.tables where table_name=%s)", (a,))
                if curr.fetchone()[0] == True:
                    print("Table Renamed Successfully...")
                else:
                    print("Sorry!!! table doesn't exist")
                curr.execute("ALTER TABLE IF EXISTS "+a+" RENAME TO "+b+";")
                
    def rename_field(self,a:String,b:String,c:String):
        with connect(**self.postgres_config) as conn:
            conn.autocommit = True
            with conn.cursor() as curr:
                curr.execute("select exists(select * from information_schema.tables where table_name=%s)", (a,))
                if curr.fetchone()[0] == False:
                    print("Sorry!!! Table doesn't exist")
                else:
                    curr.execute("select exists(select * from information_schema.columns where table_name=%s and column_name=%s)", (a,b,))
                    if curr.fetchone()[0] == True:
                        curr.execute("ALTER TABLE "+a+" RENAME COLUMN "+b+" TO "+c+";")
                        print("Coulumn Renamed Successfully...")
                    else:
                        print("Sorry!!! Column doesn't exist")
    def change_datatype(self,a:String,b:String,c:String):
        with connect(**self.postgres_config) as conn:
            conn.autocommit=True
            with conn.cursor() as curr:
                curr.execute(
                    "select exists(select * from information_schema.tables where table_name=%s)", (a,))
                if curr.fetchone()[0] == False:
                    print("Sorry!!! Table doesn't exist")
                else:
                    curr.execute(
                        "select exists(select * from information_schema.columns where table_name=%s and column_name=%s)", (a, b,))
                    if curr.fetchone()[0] == True:
                        curr.execute("ALTER TABLE "+a+
                                     " ALTER COLUMN "+b+" TYPE "+c+";")
                        print("Coulumn Datatype Changed Successfully...")
                    else:
                        print("Sorry!!! Column doesn't exist")
        
                

if __name__ == "__main__":
    db_obj = DBPlay()
    map_dict = {
        1:db_obj.create_table,
        2:db_obj.drop_table,
        3:db_obj.rename_table,
        4:db_obj.rename_field,
        5:db_obj.change_datatype
    }
    print("Connected to db")
    while(True):
        print("Choose 1:create Table 2:Drop Table 3:Rename Table 4:Rename Column 0:Exit from database")
        s = int(input("what u want! \n"))
        if s == 0:
            print("Disconnected from db")
        elif s == 1:
            map_dict[s](input("Table Name:"),int(input("Number of Fields:")))
        elif s == 2:
            map_dict[s](input("Table Name:"))
        elif s == 3:
            map_dict[s](input("Old Name:"),input("New Name:"))
        elif s == 4:
            map_dict[s](input("Table Name:"),input("Old Name:"), input("New Name:"))
        elif s == 5:
            map_dict[s](input("Table Name:"),input("Column Name:"), input("Datatype:"))
            
        
            