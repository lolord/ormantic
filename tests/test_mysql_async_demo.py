import pytest

from ormantic import Field, Model
from ormantic.dialects.mysql import create_client

"""CREATE TABLE `users` (
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
async def test_mysql_async_demo(mysql_config: dict):
    client = await create_client(**mysql_config)
    async with client:
        async with client.session() as session:
            # insert
            tom = User(id=1, name="tom", email="tom@email.com", password="123456")
            jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
            await session.save_all([tom, jerry])

            # query
            tom = await session.find_one(User, User.id == 1)
            assert tom

            # update
            new_pwd = "654321"
            tom.password = new_pwd
            await session.save(tom)
            tom = await session.find_one(User, User.id == 1)
            assert tom and tom.password == new_pwd

            # delete
            await session.remove(tom)
