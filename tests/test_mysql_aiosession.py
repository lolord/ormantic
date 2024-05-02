import pytest

from ormantic import Delete, Field, Model
from ormantic.dialects.mysql import create_client
from ormantic.errors import RowNotFoundError

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
    tom = User(id=1, name="tom", email="tom@email.com", password="123456")
    jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
    await session.save_all([tom, jerry])
    count = await session.count(User)
    assert count == 2
    tom = await session.find_one(User, User.id == 1)
    assert tom
    tom.name = "Tom"
    await session.save(tom)
    tom = await session.find_one(User, User.id == 1)
    assert tom
    assert tom.name == "Tom"
    await session.commit()
    await session.close()
    await client.close()


@pytest.mark.asyncio
async def test_mysql_curd_with_context():
    client = await create_client(
        host="192.168.56.101",
        port=3306,
        user="root",
        password="123456",
        db="test",
    )
    async with client:
        async with client.session() as session:
            count = await session.delete(Delete(User))
            tom = User(id=1, name="tom", email="tom@email.com", password="123456")
            jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
            await session.save_all([tom, jerry])
            count = await session.count(User)
            assert count == 2

            names = await session.distinct(User, User.name, sorts=[User.name.asc])  # type: ignore
            assert names == ["jerry", "tom"]

            users: list[User] = []
            async for i in session.find(User, sorts=[User.name.asc]):  # type: ignore
                users.append(i)
            assert names == [i.name for i in users]

            users = await session.find(User, sorts=[User.name.asc])  # type: ignore
            assert names == [i.name for i in users]

            tom = await session.find_one(User, User.id == 1)
            assert tom
            tom.name = "Tom"
            await session.save(tom)
            # Nothing to do
            await session.save(tom)

            await session.remove(tom)
            with pytest.raises(RowNotFoundError):
                await session.remove(tom)
            tom = await session.find_one(User, User.id == 1)
            assert tom is None
