import pytest

from ormantic import Field, Model, PrimaryKeyModifyError
from ormantic.dialects.mysql.client import AsyncClient
from ormantic.dialects.mysql.engine import AIOEngine

"""CREATE TABLE `users` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `email` varchar(255) COLLATE utf8_bin NOT NULL,
    `password` varchar(255) COLLATE utf8_bin NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
AUTO_INCREMENT=1 ;"""


class User(Model):
    id: int = Field(primary=True, autoincrement=True)
    email: str
    password: str
    __table__ = "users"


@pytest.mark.asyncio
async def test_mysql_engine():
    client = AsyncClient(
        host="192.168.56.101",
        port=3306,
        user="root",
        password="123456",
        db="test",
    )

    engine = AIOEngine(client)
    id = 1
    email = "xxx@xxx.com"
    password = "123456"
    user = User(id=id, email=email, password=password)

    # save user to db
    await engine.save(user)

    # query user from the db
    user = await engine.find_one(User, User.id == id)  # type: ignore
    assert user is not None
    assert user.id == id
    assert user.email == email
    assert user.password == password

    new_pwd = "654321"
    user.password = new_pwd
    # modify user and save them to the db
    await engine.save(user)

    # query updated user
    user = await engine.find_one(User, User.id == id)  # type: ignore
    assert user is not None
    assert user.id == id
    assert user.email == email
    assert user.password == new_pwd

    # Delete user from database
    await engine.delete(user)  # type: ignore

    # user does not exist
    user = await engine.find_one(User, User.id == id)  # type: ignore
    assert user is None

    # auto-increment id
    user = User(email=email, password=password)  # type: ignore
    assert user.id is None

    await engine.save(user)
    # update auto-increment id after saving
    assert user.id is not None

    inc_id = user.id

    # the user exists
    user = await engine.find_one(User, User.id == inc_id)  # type: ignore
    assert user is not None
    assert user.id == inc_id

    # once the primary key is assigned, it cannot be modified
    with pytest.raises(PrimaryKeyModifyError):
        user.id = user.id + 1

    await engine.close()
