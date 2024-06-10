# ormantic

ORM(Object Relational Mapping) for relational database based on standard python type hints. It's built on top of pydantic for model definition and validation.

---

## Features

1. **Easy**: If you know how to use `orm`, then you will also use it.
2. **Query**: Structured query objects can ignore differences between databases.
3. **Validate**: Pydantic can help with data validation.

## Requirements

**Python**: 3.11 and later
**Pydantic**: ~=1.10

## Installation

``` shell
pip install git+https://github.com/lolord/ormantic.git
```

## Basic Usage

### Synchronous

``` python
import pytest

from ormantic import Field, Model
from ormantic.dialects.mysql import Client
from ormantic.dialects.mysql.session import ConnectCreator

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


@pytest.fixture(scope="function")
def mysql_sync_client(mysql_config: dict):
    factory = ConnectCreator(**mysql_config)
    return Client(factory, mincached=1, maxconnections=5)


def test_mysql_sync_demo(mysql_sync_client: Client):
    with mysql_sync_client:
        with mysql_sync_client.session(autocommit=True) as session:
            # insert
            tom = User(id=1, name="tom", email="tom@email.com", password="123456")
            jerry = User(name="jerry", email="jerry@email.com", password="123456")  # type: ignore
            session.save_all([tom, jerry])

            # query
            tom = session.find_one(User, User.id == 1)
            assert tom

            # update
            new_pwd = "654321"
            tom.password = new_pwd
            session.save(tom)
            tom = session.find_one(User, User.id == 1)
            assert tom and tom.password == new_pwd

            # delete
            session.remove(tom)

```

### Asynchronous

```python
import pytest

from ormantic import Field, Model, PrimaryKeyModifyError
from ormantic.dialects.mysql import AIOClient, create_client
from ormantic.dialects.mysql.session import ConnectCreator

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

```

## Implemented database dialects

- [X] MySQL
  - [X] aiomysql
  - [X] pymysql
- [ ] PostgreSQL
  - [X] psycopg2
  - [ ] asyncpg
- [ ] SQLite
