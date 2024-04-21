import pytest

from ormantic import Delete, Field, Model
from ormantic.dialects.mysql import create_client

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


@pytest.mark.asyncio
async def test_mysql_curd():
    client = await create_client(
        host="192.168.56.101",
        port=3306,
        user="root",
        password="123456",
        db="test",
    )
    session = await client.session()
    # await session.execute("drop table `users`;")
    # await session.execute(table)
    # await session.commit()

    count = await session.delete(Delete(User))
    print("count", count)
    tom = User(id=1, name="tom", email="tom@email.com", password="123456")
    jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
    await session.save_all([tom, jerry])
    count = await session.count(User)
    assert count == 2
    tom = await session.find_one(User, User.id == 1)
    tom.name = "Tom"
    await session.save(tom)
    tom = await session.find_one(User, User.id == 1)
    assert tom.name == "Tom"
    await session.close()
    await client.close()
