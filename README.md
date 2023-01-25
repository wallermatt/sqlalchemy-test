# sqlalchemy-test

# Create Engine

from sqlalchemy import create_engine

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)

engine = create_engine("sqlite+pysqlite:///:memory:")


# Getting a connection

from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("select 'hello world'"))
    print(result.all())

BEGIN (implicit)
select 'hello world'
[...] ()

[('hello world',)]
ROLLBACK

# Committing changes

# "commit as you go"
with engine.connect() as conn:
    conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
    )
    conn.commit()

BEGIN (implicit)
CREATE TABLE some_table (x int, y int)
[...] ()
<sqlalchemy.engine.cursor.CursorResult object at 0x...>
INSERT INTO some_table (x, y) VALUES (?, ?)
[...] ((1, 1), (2, 4))
<sqlalchemy.engine.cursor.CursorResult object at 0x...>
COMMIT

# Insert rows

with engine.begin() as conn:
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 6, "y": 8}, {"x": 9, "y": 10}],
    )

BEGIN (implicit)
INSERT INTO some_table (x, y) VALUES (?, ?)
[...] ((6, 8), (9, 10))
<sqlalchemy.engine.cursor.CursorResult object at 0x...>
COMMIT

with engine.connect() as conn:
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 11, "y": 12}, {"x": 13, "y": 14}],
    )
    conn.commit()

BEGIN (implicit)
INSERT INTO some_table (x, y) VALUES (?, ?)
[...] ((11, 12), (13, 14))
<sqlalchemy.engine.cursor.CursorResult object at 0x...>
COMMIT

# Fetching rows

with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table"))
    for row in result:
        print(f"x: {row.x} y: {row.y}")


with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table WHERE y > :y"), {"y": 2})
    for row in result:
        print(f"x: {row.x}  y: {row.y}")

BEGIN (implicit)
SELECT x, y FROM some_table WHERE y > ?
[...] (2,)
x: 2  y: 4
x: 6  y: 8
x: 9  y: 10
ROLLBACK

https://docs.sqlalchemy.org/en/14/tutorial/metadata.html


with engine.connect() as conn:
    conn.execute(
        text("INSERT INTO user_account (name, fullname) VALUES (:name, :fullname)"),
        [{"name": "Matt", "fullname": "Matthew Waller"}, {"name": "Kev", "fullname": "Kevin Clifton"}],
    )
    conn.commit()


with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM user_account"))
    columns = user_table.c.keys()
    for row in result:
        #print(f"x: {row.x} y: {row.y}")
        print([f"{columns[i]}: {row[i]}" for i,_ in enumerate(columns)])


from sqlalchemy import MetaData
metadata_obj = MetaData()
metadata_obj.create_all(engine)

from sqlalchemy import Table, Column, Integer, String
user_table = Table(
    "user_account",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String(30)),
    Column("fullname", String),
)


from sqlalchemy import ForeignKey
address_table = Table(
    "address",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("user_id", ForeignKey("user_account.id"), nullable=False),
    Column("email_address", String, nullable=False),
)


from sqlalchemy.orm import registry
mapper_registry = registry()

Base = mapper_registry.generate_base()




from sqlalchemy.orm import relationship
class User(Base):
    __tablename__ = "user_account"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    fullname = Column(String)
    addresses = relationship("Address", back_populates="user")
    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"

class Address(Base):
    __tablename__ = "address"
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("user_account.id"))
    user = relationship("User", back_populates="addresses")
    def __repr__(self):
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"



Address.__table__

sandy = User(name="sandy", fullname="Sandy Cheeks")

sandy <- no id

Base.metadata.create_all(engine)

# Table reflection

some_table = Table("some_table", metadata_obj, autoload_with=engine)


# Insert

from sqlalchemy import insert

user_table = User.__table__

insert_stmt = insert(user_table).values(name="spongebob", fullname="Spongebob Squarepants")



compiled = stmt.compile()
compiled.params

with engine.connect() as conn:
    result = conn.execute(insert_stmt)
    conn.commit()




with engine.connect() as conn:
    result = conn.execute(
        insert(user_table),
        [
            {"name": "sandy", "fullname": "Sandy Cheeks"},
            {"name": "patrick", "fullname": "Patrick Star"},
        ],
    )
    conn.commit()




select_stmt = select(user_table.c.id, user_table.c.name + "spongebob")
insert_stmt = insert(address_table).from_select(
    ["user_id", "email_address"], select_stmt
)
print(insert_stmt)


print(insert_stmt.returning(address_table.c.id, address_table.c.email_address))

# Selecting rows with core ORM

from sqlalchemy import select
stmt = select(user_table).where(user_table.c.name == "spongebob")
print(stmt)


with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)


from sqlalchemy.orm import sessionmaker
Session = sessionmaker(engine)


stmt = select(User).where(User.name == "spongebob")
with Session() as session:
    for row in session.execute(stmt):
        print(row)

(User(id=1, name='spongebob', fullname='Spongebob Squarepants'),)

print(select(user_table.c.name, user_table.c.fullname))

print(select(User))

row = session.execute(select(User)).first()  <- (User(id=1, name='spongebob', fullname='spongebob squarepants'),)

user = session.scalars(select(User)).first()  <- returns one object, didn't work well with join


print(
    select(user_table.c.name, address_table.c.email_address).join_from(
        user_table, address_table
    )
)
SELECT user_account.name, address.email_address
FROM user_account JOIN address ON user_account.id = address.user_id

print(select(user_table.c.name, address_table.c.email_address).join(address_table))
SELECT user_account.name, address.email_address
FROM user_account JOIN address ON user_account.id = address.user_id


row = session.execute(stmt).first() 


from sqlalchemy import update
stmt = (
    update(user_table)
    .where(user_table.c.name == "Matt")
    .values(fullname="Matthew the Star")
)
print(stmt)
UPDATE user_account SET fullname=:fullname WHERE user_account.name = :name_1

from sqlalchemy import bindparam
stmt = (
    update(user_table)
    .where(user_table.c.name == bindparam("oldname"))
    .values(name=bindparam("newname"))
)
with engine.begin() as conn:
    conn.execute(
        stmt,
        [
            {"oldname": "jack", "newname": "ed"},
            {"oldname": "wendy", "newname": "mary"},
            {"oldname": "jim", "newname": "jake"},
        ],
    )
BEGIN (implicit)
UPDATE user_account SET name=? WHERE user_account.name = ?
[...] (('ed', 'jack'), ('mary', 'wendy'), ('jake', 'jim'))
<sqlalchemy.engine.cursor.CursorResult object at 0x...>
COMMIT

from sqlalchemy import delete
stmt = delete(user_table).where(user_table.c.name == "patrick")
print(stmt)
DELETE FROM user_account WHERE user_account.name = :name_1

