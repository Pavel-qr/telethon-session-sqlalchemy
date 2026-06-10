from typing import Optional, Tuple, Any, Union
import datetime

from sqlalchemy import and_, select

from telethon.sessions.memory import _SentFileType
from telethon import utils
from telethon.crypto import AuthKey
from telethon.tl.types import InputPhoto, InputDocument, PeerUser, PeerChat, PeerChannel, updates

from .orm import AlchemySession


class AlchemyCoreSession(AlchemySession):
    """Core-режим: каждая операция выполняется напрямую через engine в собственной
    транзакции (engine.begin()/connect()), без общей ORM-Session. Поэтому ошибка
    одного клиента не «отравляет» сессию остальных и не нужен общий rollback.

    Совместимо с SQLAlchemy 2.0 (Engine.execute() и select([...]) удалены)."""

    def _load_session(self) -> None:
        t = self.Session.__table__
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t.c.dc_id, t.c.server_address, t.c.port, t.c.auth_key)
                .where(t.c.session_id == self.session_id)
            ).first()
        if row is not None:
            self._dc_id, self._server_address, self._port, auth_key = row
            self._auth_key = AuthKey(data=auth_key)

    def _get_auth_key(self) -> Optional[AuthKey]:
        t = self.Session.__table__
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t.c.auth_key).where(t.c.session_id == self.session_id)
            ).first()
        ak = row[0] if row is not None else None
        return AuthKey(data=ak) if ak else None

    def get_update_state(self, entity_id: int) -> Optional[updates.State]:
        t = self.UpdateState.__table__
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t).where(and_(t.c.session_id == self.session_id,
                                     t.c.entity_id == entity_id))
            ).first()
        if row is None:
            return None
        _, _, pts, qts, date, seq, unread_count = row
        date = datetime.datetime.utcfromtimestamp(date)
        return updates.State(pts, qts, date, seq, unread_count)

    def set_update_state(self, entity_id: int, row: Any) -> None:
        t = self.UpdateState.__table__
        with self.engine.begin() as conn:
            conn.execute(t.delete().where(and_(t.c.session_id == self.session_id,
                                               t.c.entity_id == entity_id)))
            conn.execute(t.insert().values(
                session_id=self.session_id, entity_id=entity_id, pts=row.pts,
                qts=row.qts, date=row.date.timestamp(), seq=row.seq,
                unread_count=row.unread_count))

    def _update_session_table(self) -> None:
        t = self.Session.__table__
        with self.engine.begin() as conn:
            conn.execute(t.delete().where(t.c.session_id == self.session_id))
            conn.execute(t.insert().values(
                session_id=self.session_id, dc_id=self._dc_id,
                server_address=self._server_address, port=self._port,
                auth_key=(self._auth_key.key if self._auth_key else b'')))

    def save(self) -> None:
        # Каждая операция уже закоммичена в своей транзакции (engine.begin()).
        pass

    def delete(self) -> None:
        with self.engine.begin() as conn:
            for table in (self.Session.__table__, self.Entity.__table__,
                          self.SentFile.__table__, self.UpdateState.__table__):
                conn.execute(table.delete().where(table.c.session_id == self.session_id))

    def _entity_values_to_row(self, id: int, hash: int, username: str, phone: str, name: str
                              ) -> Any:
        return id, hash, username, phone, name

    def process_entities(self, tlo: Any) -> None:
        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        t = self.Entity.__table__
        with self.engine.begin() as conn:
            conn.execute(t.delete().where(and_(t.c.session_id == self.session_id,
                                               t.c.id.in_([row[0] for row in rows]))))
            conn.execute(t.insert(), [dict(session_id=self.session_id, id=row[0], hash=row[1],
                                           username=row[2], phone=row[3], name=row[4])
                                      for row in rows])

    def get_entity_rows_by_phone(self, key: str) -> Optional[Tuple[int, int]]:
        return self._get_entity_rows_by_condition(self.Entity.__table__.c.phone == key)

    def get_entity_rows_by_username(self, key: str) -> Optional[Tuple[int, int]]:
        return self._get_entity_rows_by_condition(self.Entity.__table__.c.username == key)

    def get_entity_rows_by_name(self, key: str) -> Optional[Tuple[int, int]]:
        return self._get_entity_rows_by_condition(self.Entity.__table__.c.name == key)

    def _get_entity_rows_by_condition(self, condition) -> Optional[Tuple[int, int]]:
        t = self.Entity.__table__
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t.c.id, t.c.hash)
                .where(and_(t.c.session_id == self.session_id, condition))
            ).first()
        return tuple(row) if row is not None else None

    def get_entity_rows_by_id(self, key: int, exact: bool = True) -> Optional[Tuple[int, int]]:
        t = self.Entity.__table__
        if exact:
            condition = t.c.id == key
        else:
            ids = (
                utils.get_peer_id(PeerUser(key)),
                utils.get_peer_id(PeerChat(key)),
                utils.get_peer_id(PeerChannel(key))
            )
            condition = t.c.id.in_(ids)
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t.c.id, t.c.hash)
                .where(and_(t.c.session_id == self.session_id, condition))
            ).first()
        return tuple(row) if row is not None else None

    def get_file(self, md5_digest: str, file_size: int, cls: Any) -> Optional[Tuple[int, int]]:
        t = self.SentFile.__table__
        with self.engine.connect() as conn:
            row = conn.execute(
                select(t.c.id, t.c.hash).where(and_(
                    t.c.session_id == self.session_id,
                    t.c.md5_digest == md5_digest,
                    t.c.file_size == file_size,
                    t.c.type == _SentFileType.from_type(cls).value))
            ).first()
        return tuple(row) if row is not None else None

    def cache_file(self, md5_digest: str, file_size: int,
                   instance: Union[InputDocument, InputPhoto]) -> None:
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache {} instance".format(type(instance)))

        t = self.SentFile.__table__
        file_type = _SentFileType.from_type(type(instance)).value
        with self.engine.begin() as conn:
            conn.execute(t.delete().where(and_(
                t.c.session_id == self.session_id,
                t.c.md5_digest == md5_digest,
                t.c.type == file_type,
                t.c.file_size == file_size)))
            conn.execute(t.insert().values(
                session_id=self.session_id, md5_digest=md5_digest,
                type=file_type, file_size=file_size, id=instance.id,
                hash=instance.access_hash))
