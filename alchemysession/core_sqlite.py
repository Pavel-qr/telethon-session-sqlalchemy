from typing import Any, Union

from sqlalchemy import text

from telethon.sessions.memory import _SentFileType
from telethon.tl.types import InputPhoto, InputDocument

from .core import AlchemyCoreSession


class AlchemySQLiteCoreSession(AlchemyCoreSession):
    def set_update_state(self, entity_id: int, row: Any) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT OR REPLACE INTO {} "
                     "(session_id, entity_id, pts, qts, date, seq, unread_count) "
                     "VALUES (:session_id, :entity_id, :pts, :qts, :date, :seq, :unread_count)"
                     .format(self.UpdateState.__tablename__)),
                dict(session_id=self.session_id, entity_id=entity_id, pts=row.pts,
                     qts=row.qts, date=row.date.timestamp(), seq=row.seq,
                     unread_count=row.unread_count))

    def process_entities(self, tlo: Any) -> None:
        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT OR REPLACE INTO {} "
                     "(session_id, id, hash, username, phone, name) "
                     "VALUES (:session_id, :id, :hash, :username, :phone, :name)"
                     .format(self.Entity.__tablename__)),
                [dict(session_id=self.session_id, id=row[0], hash=row[1],
                      username=row[2], phone=row[3], name=row[4])
                 for row in rows])

    def cache_file(self, md5_digest: str, file_size: int,
                   instance: Union[InputDocument, InputPhoto]) -> None:
        if not isinstance(instance, (InputDocument, InputPhoto)):
            raise TypeError("Cannot cache {} instance".format(type(instance)))

        with self.engine.begin() as conn:
            conn.execute(
                text("INSERT OR REPLACE INTO {} "
                     "(session_id, md5_digest, file_size, type, id, hash) "
                     "VALUES (:session_id, :md5_digest, :file_size, :type, :id, :hash)"
                     .format(self.SentFile.__tablename__)),
                dict(session_id=self.session_id, md5_digest=md5_digest, file_size=file_size,
                     type=_SentFileType.from_type(type(instance)).value,
                     id=instance.id, hash=instance.access_hash))
