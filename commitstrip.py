# commitstrip - A maubot plugin to view CommitStrips
# Copyright (C) 2019 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from typing import Awaitable, Optional, Type, List, Iterable, Tuple, Dict, Any
from difflib import SequenceMatcher
from datetime import datetime, timezone
from html import escape
import re
import asyncio
import random

from sqlalchemy import Column, String, Integer, Text, DateTime, orm
from sqlalchemy.ext.declarative import declarative_base
from attr import dataclass

from mautrix.types import ContentURI, RoomID, UserID, ImageInfo, SerializableAttrs
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from maubot import Plugin, MessageEvent
from maubot.handlers import command


@dataclass
class CommitInfo(SerializableAttrs['CommitInfo']):
    id: int
    image_id: int
    title: str
    date: datetime


class MediaCache:
    __tablename__ = "media_cache"
    query: orm.Query = None

    id: int = Column(Integer, primary_key=True)
    url: str = Column(String(255))
    mxc_uri: ContentURI = Column(String(255))
    file_name: str = Column(String(255))
    mime_type: str = Column(String(255))
    width: int = Column(Integer)
    height: int = Column(Integer)
    size: int = Column(Integer)

    def __init__(self, id: int, url: str, mxc_uri: ContentURI, file_name: str, mime_type: str,
                 width: int, height: int, size: int) -> None:
        self.id = id
        self.url = url
        self.mxc_uri = mxc_uri
        self.file_name = file_name
        self.mime_type = mime_type
        self.width = width
        self.height = height
        self.size = size


class CommitIndex:
    __tablename__ = "commit_index"
    query: orm.Query = None

    id: int = Column(Integer, primary_key=True)
    image_id: int = Column(Integer)
    title: str = Column(Text)
    date: datetime = Column(DateTime)

    def __init__(self, id: int, image_id: int, title: str, date: datetime) -> None:
        self.id = id
        self.image_id = image_id
        self.title = title
        self.date = date

    def __lt__(self, other: 'CommitIndex') -> bool:
        return self.id > other.id

    def __gt__(self, other: 'CommitIndex') -> bool:
        return self.id < other.id

    def __eq__(self, other: 'CommitIndex') -> bool:
        return self.id == other.id


class Subscriber:
    __tablename__ = "subscriber"
    query: orm.Query = None

    room_id: RoomID = Column(String(255), primary_key=True)
    requested_by: UserID = Column(String(255))

    def __init__(self, room_id: RoomID, requested_by: UserID) -> None:
        self.room_id = room_id
        self.requested_by = requested_by


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("inline")
        helper.copy("poll_interval")
        helper.copy("spam_sleep")
        helper.copy("allow_reindex")
        helper.copy("max_search_results")
        helper.copy("base_command")


class CommitBot(Plugin):
    media_cache: Type[MediaCache]
    subscriber: Type[Subscriber]
    commit_index: Type[CommitIndex]
    db: orm.Session
    latest_id: int
    poll_task: asyncio.Future

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    async def start(self) -> None:
        await super().start()
        self.config.load_and_update()
        db_factory = orm.sessionmaker(bind=self.database)
        db_session = orm.scoped_session(db_factory)

        base = declarative_base()
        base.metadata.bind = self.database

        class MediaCacheImpl(MediaCache, base):
            query = db_session.query_property()

        class CommitIndexImpl(CommitIndex, base):
            query = db_session.query_property()

        class SubscriberImpl(Subscriber, base):
            query = db_session.query_property()

        self.media_cache = MediaCacheImpl
        self.subscriber = SubscriberImpl
        self.commit_index = CommitIndexImpl
        base.metadata.create_all()

        self.db = db_session
        self.latest_id = 0

        self.poll_task = asyncio.ensure_future(self.poll_commit(), loop=self.loop)

    async def stop(self) -> None:
        await super().stop()
        self.poll_task.cancel()

    def _index_info(self, info: CommitInfo) -> None:
        self.db.merge(self.commit_index(info.id, info.image_id, info.title, info.date))
        self.db.commit()

    def _parse_commit_info(self, info: Dict[str, Any]) -> Optional[CommitInfo]:
        try:
            date = datetime.strptime(info["date_gmt"], "%Y-%m-%dT%H:%M:%S")
            date = date.replace(tzinfo=timezone.utc)
            id: int = info["id"]
            title: str = info["title"]["rendered"]
            content: str = info["content"]["rendered"]
            try:
                image_id_match = re.search(r"wp-image-(\d+)", content)
                image_id = int(image_id_match.group(1))
            except (AttributeError, TypeError):
                image_id = info["featured_media"]
            return CommitInfo(id=id, image_id=image_id, title=title, date=date)
        except Exception:
            self.log.exception("Failed to parse info from %s", info)
            return None

    async def _get_commit_info(self, url, handle_data) -> Optional[CommitInfo]:
        resp = await self.http.get(url)
        if resp.status == 200:
            data = await resp.json()
            info = self._parse_commit_info(handle_data(data))
            if info:
                self._index_info(info)
                return info
            return None
        resp.raise_for_status()
        return None

    def get_latest_commit(self) -> Awaitable[Optional[CommitInfo]]:
        return self._get_commit_info("http://www.commitstrip.com/en/wp-json/wp/v2/posts?per_page=1",
                                     lambda data: data[0])

    def get_commit(self, num: int) -> Awaitable[Optional[CommitInfo]]:
        return self._get_commit_info(f"http://www.commitstrip.com/en/wp-json/wp/v2/posts/{num}",
                                     lambda data: data)

    async def _get_media_info(self, image_id: int) -> Optional[MediaCache]:
        cache = self.media_cache.query.get(image_id)
        if cache is not None:
            return cache

        resp = await self.http.get(f"https://www.commitstrip.com/en/wp-json/wp/v2/media/{image_id}")
        if resp.status != 200:
            self.log.warning(f"Unexpected status fetching info for image {image_id}: {resp.status}")
            return None

        info = await resp.json()
        url = info["source_url"]
        mime = info["mime_type"]
        details = info["media_details"]
        width, height = details["width"], details["height"]
        filename = url.split("/")[-1]

        data_resp = await self.http.get(url)
        if data_resp.status != 200:
            self.log.warning(f"Unexpected status fetching image {image_id}: {resp.status}")
            return None

        data = await data_resp.read()
        uri = await self.client.upload_media(data, mime_type=mime, filename=filename)
        cache = self.media_cache(id=image_id, url=url, mxc_uri=uri, file_name=filename,
                                 mime_type=mime, width=width, height=height, size=len(data))
        self.db.add(cache)
        self.db.commit()
        return cache

    async def send_commit(self, room_id: RoomID, commit: CommitInfo) -> None:
        try:
            await self._send_commit(room_id, commit)
        except Exception:
            self.log.exception(f"Failed to send CommitStrip {commit.id} to {room_id}")

    async def _send_commit(self, room_id: RoomID, commit: CommitInfo) -> None:
        info = await self._get_media_info(commit.image_id)
        if self.config["format"] == "inline":
            await self.client.send_text(room_id, text=f"# {commit.title}\n{info.url}",
                                        html=(f"<h1>{escape(commit.title)}</h1><br/>"
                                              f"<img src='{info.mxc_uri}'"
                                              f"     title='{info.file_name}'/>"))
        elif self.config["format"] in ("separate", "filename"):
            if self.config["format"] == "separate":
                filename = info.file_name
                await self.client.send_text(room_id, text=f"# {commit.title}",
                                            html=f"<h1>{escape(commit.title)}</h1>")
            else:
                filename = info.title
            await self.client.send_image(room_id, url=info.mxc_uri, file_name=filename,
                                         info=ImageInfo(
                                             mimetype=info.mime_type,
                                             size=info.size,
                                             width=info.width,
                                             height=info.height,
                                         ))
        else:
            self.log.error(f"Unknown format \"{self.config['format']}\" specified in config.")

    async def broadcast(self, commit: CommitInfo) -> None:
        self.log.debug(f"Broadcasting CommitStrip {commit.title}")
        subscribers = list(self.subscriber.query.all())
        random.shuffle(subscribers)
        spam_sleep = self.config["spam_sleep"]
        if spam_sleep < 0:
            await asyncio.gather(*[self.send_commit(sub.room_id, commit)
                                   for sub in subscribers],
                                 loop=self.loop)
        else:
            for sub in subscribers:
                await self.send_commit(sub.room_id, commit)
                if spam_sleep > 0:
                    await asyncio.sleep(spam_sleep)

    async def poll_commit(self) -> None:
        try:
            await self._poll_commit()
        except asyncio.CancelledError:
            self.log.debug("Polling stopped")
            pass
        except Exception:
            self.log.exception("Failed to poll CommitStrip")

    async def _poll_commit(self) -> None:
        self.log.debug("Polling started")
        latest = await self.get_latest_commit()
        self.latest_id = latest.id
        while True:
            latest = await self.get_latest_commit()
            if latest.id > self.latest_id:
                self.latest_id = latest.id
                await self.broadcast(latest)
            await asyncio.sleep(self.config["poll_interval"], loop=self.loop)

    @command.new(name=lambda self: self.config["base_command"],
                 help="Search for a commit and view the first result, or view the latest commit",
                 require_subcommand=False, arg_fallthrough=False)
    @command.argument("query", required=False)
    async def commit(self, evt: MessageEvent, query: Optional[str]) -> None:
        if query:
            results = self._search(query)
            if not results:
                await evt.reply("No results :(")
                return
            result = results[0][0]
            commit = await self.get_commit(result.id)
            if not commit:
                await evt.reply(f"Found result {result.title}, but failed to fetch content")
                return
        else:
            commit = await self.get_latest_commit()
        await self.send_commit(evt.room_id, commit)

    @commit.subcommand("reindex", help="Fetch and store info about every CommitStrip to date for "
                                       "searching.")
    async def reindex(self, evt: MessageEvent) -> None:
        if not self.config["allow_reindex"]:
            await evt.reply("Sorry, the reindex command has been disabled on this instance.")
            return
        self.config["allow_reindex"] = False
        self.config.save()
        await evt.reply("Reindexing commit database...")
        page = 1
        indexed = 0
        failed = 0
        while True:
            resp = await self.http.get("http://www.commitstrip.com/en/wp-json/wp/v2/posts"
                                       f"?per_page=100&page={page}")
            data = await resp.json()
            if not isinstance(data, list):
                self.log.debug("Response not a list, stopping indexing %s", data)
                break
            local_indexed = 0
            local_failed = 0
            for commit in data:
                info = self._parse_commit_info(commit)
                if info:
                    self._index_info(info)
                    local_indexed += 1
                else:
                    local_failed += 1
            self.log.debug(f"Indexed {local_indexed} commits and failed to index {local_failed} "
                           f"commits on page {page}")
            indexed += local_indexed
            failed += local_failed
            page += 1
        await evt.reply(f"Reindexing complete. Indexed {indexed} commits and failed to "
                        f"index {failed} commits.")

    def _index_similarity(self, result: CommitIndex, query: str) -> float:
        query = query.lower()
        title_sim = SequenceMatcher(None, result.title.strip().lower(), query).ratio()
        return round(title_sim * 100, 1)

    def _sort_search_results(self, results: List[CommitIndex], query: str
                             ) -> Iterable[Tuple[CommitIndex, float]]:
        similarity = (self._index_similarity(result, query) for result in results)
        return ((result, similarity) for similarity, result
                in sorted(zip(similarity, results), reverse=True))

    def _search(self, query: str) -> Optional[List[Tuple[CommitIndex, float]]]:
        sql_query = f"%{query}%"
        results = self.commit_index.query.filter(self.commit_index.title.like(sql_query)).all()
        if len(results) == 0:
            return None
        return list(self._sort_search_results(results, query))

    @commit.subcommand("search", help="Search for a relevant CommitStrip")
    @command.argument("query", pass_raw=True)
    async def search(self, evt: MessageEvent, query: str) -> None:
        results = self._search(query)
        if not results:
            await evt.reply("No results :(")
            return
        msg = "Results:\n\n"
        more_results = None
        limit = self.config["max_search_results"]
        if len(results) > limit:
            more_results = len(results) - limit, results[limit][1]
            results = results[:limit]
        msg += "\n".join(f"* [{result.title}](https://www.commitstrip.com/?p={result.id})"
                         f"  ({similarity} % match)"
                         for result, similarity in results)
        if more_results:
            number, similarity = more_results
            msg += (f"\n\nThere were {number} other results "
                    f"with a similarity lower than {similarity + 0.1} %")
        await evt.reply(msg)

    @commit.subcommand("subscribe", help="Subscribe to CommitStrip updates")
    async def subscribe(self, evt: MessageEvent) -> None:
        sub = self.subscriber.query.get(evt.room_id)
        if sub is not None:
            await evt.reply("This room has already been subscribed to "
                            f"CommitStrip updates by {sub.requested_by}")
            return
        sub = self.subscriber(evt.room_id, evt.sender)
        self.db.add(sub)
        self.db.commit()
        await evt.reply("Subscribed to CommitStrip updates successfully!")

    @commit.subcommand("unsubscribe", help="Unsubscribe from CommitStrip updates")
    async def unsubscribe(self, evt: MessageEvent) -> None:
        sub = self.subscriber.query.get(evt.room_id)
        if sub is None:
            await evt.reply("This room is not subscribed to CommitStrip updates.")
            return
        self.db.delete(sub)
        self.db.commit()
        await evt.reply("Unsubscribed from CommitStrip updates successfully :(")
