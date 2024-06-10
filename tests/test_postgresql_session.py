import pytest

from ormantic import Delete, Field, Model
from ormantic.dialects.postgresql.session import Client, ConnectCreator
from ormantic.errors import RowNotFoundError

table = """CREATE SEQUENCE users_id_seq INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;

CREATE TABLE "public"."users" (
    "id" integer DEFAULT nextval('users_id_seq') NOT NULL,
    "name" character varying(255) NOT NULL,
    "email" character varying NOT NULL,
    "password" character varying NOT NULL,
    CONSTRAINT "users_pkey" PRIMARY KEY ("id")
) WITH (oids = false);

TRUNCATE "users";
"""


class User(Model):
    id: int = Field(primary=True, autoincrement=True)
    name: str
    email: str
    password: str
    __table__ = "users"


@pytest.fixture(scope="function")
def client(postgresql_config: dict):
    factory = ConnectCreator(**postgresql_config)
    return Client(factory, mincached=1, maxconnections=5)


# @pytest.mark.usefixtures("client")
def test_postgresql_curd(client: Client):
    session = client.session()

    count = session.delete(Delete(User))

    none = session.find_one(User)
    assert none is None

    tom = User(id=1, name="tom", email="tom@email.com", password="123456")
    jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
    session.save_all([tom, jerry])
    session.commit()
    count = session.count(User)

    assert count == 2
    names = session.distinct(User, User.name, sorts=[(User.name, True)])  # type: ignore
    assert names == [jerry.name, tom.name]

    session.commit()
    tom = session.find_one(User, User.id == 1)
    assert tom
    tom.name = "Tom"
    session.save(tom)
    # Nothing to do
    session.save(tom)
    tom = session.find_one(User, User.id == 1)
    assert tom
    assert tom.name == "Tom"
    session.remove(tom)
    tom = session.find_one(User, User.id == 1)
    assert tom is None

    session.commit()
    session.close()
    client.close()


@pytest.mark.usefixtures("client")
def test_postgresql_curd_with_context(client: Client):
    with client:
        with client.session(autocommit=True) as session:
            count = session.delete(Delete(User))
            none = session.find_one(User)
            assert none is None

            tom = User(id=1, name="tom", email="tom@email.com", password="123456")
            jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
            session.save_all([tom, jerry])
            count = session.count(User)
            assert count == 2
            names = session.distinct(User, User.name, sorts=[(User.name, True)])  # type: ignore
            assert names == [jerry.name, tom.name]

            tom = session.find_one(User, User.id == 1)
            assert tom
            tom.name = "Tom"
            session.save(tom)
            tom = session.find_one(User, User.id == 1)
            assert tom
            assert tom.name == "Tom"
            session.remove(tom)
            with pytest.raises(RowNotFoundError):
                session.remove(tom)
            tom = session.find_one(User, User.id == 1)
            assert tom is None


# @pytest.mark.skip(reason="NotImplemented")
# @pytest.mark.usefixtures("client")
# def test_postgresql_excute_insert_many(client: Client):
#     with client:
#         with client.session() as session:
#             count = session.delete(Delete(User))
#             none = session.find_one(User)
#             assert none is None

#             tom = User(id=1, name="tom", email="tom@email.com", password="123456")
#             jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
#             # Equivalent to session.save_all([tom, jerry])
#             session.execute(Insert(User, [tom, jerry]))
#             count = session.count(User)
#             assert count == 2
#             session.commit()
#             session.commit()
