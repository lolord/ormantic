import pytest

from ormantic import Delete, Field, Model
from ormantic.dialects.mysql import Client, ConnectCreator
from ormantic.errors import RowNotFoundError
from ormantic.query import Insert

table = """CREATE TABLE `users` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `name` varchar(255) COLLATE utf8_bin NOT NULL,
    `email` varchar(255) COLLATE utf8_bin NOT NULL,
    `password` varchar(255) COLLATE utf8_bin NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
AUTO_INCREMENT=1 ;"""


class User(Model):
    id: int = Field(primary=True, autoincrement=True)
    name: str
    email: str
    password: str
    __table__ = "users"


@pytest.fixture(scope="function")
def client():
    factory = ConnectCreator(
        host="192.168.56.101",
        port=3306,
        user="root",
        password="123456",
        database="test",
    )
    return Client(factory, mincached=1, maxconnections=5)


@pytest.mark.usefixtures("client")
def test_mysql_curd(client: Client):
    session = client.session()

    count = session.delete(Delete(User))
    none = session.find_one(User)
    assert none is None

    tom = User(id=1, name="tom", email="tom@email.com", password="123456")
    jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
    session.save_all([tom, jerry])
    count = session.count(User)
    assert count == 2
    names = session.distinct(User, User.name)  # type: ignore
    assert names == [tom.name, jerry.name]

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
def test_mysql_curd_with_context(client: Client):
    with client:
        with client.session() as session:
            count = session.delete(Delete(User))
            none = session.find_one(User)
            assert none is None

            tom = User(id=1, name="tom", email="tom@email.com", password="123456")
            jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
            # Equivalent to session.save_all([tom, jerry])
            session.execute(Insert(User, [tom, jerry]))
            count = session.count(User)
            assert count == 2
            names = session.distinct(User, User.name)  # type: ignore
            assert names == [tom.name, jerry.name]

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

            session.commit()
