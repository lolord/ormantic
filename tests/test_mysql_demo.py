import pytest

from ormantic import Field, Model, PrimaryKeyModifyError
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
async def test_mysql_curd():
    client = await create_client(
        host="192.168.56.101",
        port=3306,
        user="root",
        password="123456",
        db="test",
    )

    session = await client.session()

    session.execute
    id = 1
    email = "xxx@xxx.com"
    password = "123456"
    user = User(id=id, name="xxx", email=email, password=password)

    # save user to db
    await session.save(user)

    # query user from the db
    user = await session.find_one(User, User.id == id)  # type: ignore
    assert user is not None
    assert user.id == id
    assert user.email == email
    assert user.password == password

    new_pwd = "654321"
    user.password = new_pwd
    # modify user and save them to the db
    await session.save(user)

    # query updated user
    user = await session.find_one(User, User.id == id)  # type: ignore
    assert user is not None
    assert user.id == id
    assert user.email == email
    assert user.password == new_pwd

    # Delete user from database
    await session.remove(user)

    # user does not exist
    user = await session.find_one(User, User.id == id)  # type: ignore
    assert user is None

    # auto-increment id
    user = User(name="xxx", email=email, password=password)  # type: ignore
    assert user.id is None

    await session.save(user)
    # update auto-increment id after saving
    assert user.id is not None

    inc_id = user.id

    # the user exists
    user = await session.find_one(User, User.id == inc_id)  # type: ignore
    assert user is not None
    assert user.id == inc_id

    # if the primary key is not None, it cannot be modified
    with pytest.raises(PrimaryKeyModifyError):
        user.id = user.id + 1

    await session.close()
    await client.close()
