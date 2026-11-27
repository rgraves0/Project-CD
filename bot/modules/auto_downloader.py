# FILE: bot/modules/auto_downloader.py
import os
import random
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

from pyrogram import Client, filters
from pyrogram.types import Message

from config import Config
from bot.tgclient import aio
from bot.settings import bot_set
from bot.logger import LOGGER
from bot.helpers.message import check_user, user_details
from bot.helpers.tidal.tidal_api import tidalapi
from bot.helpers.tidal.handler import start_tidal
from bot.helpers.qobuz.qopy import qobuz_api
from bot.helpers.qobuz.handler import start_qobuz

CHPROTECT_URL = os.getenv("CHPROTECT_URL", "https://chprotect.thihazaw.workers.dev").rstrip("/")
SYNC_SECRET = os.getenv("SYNC_SECRET", "")

# ရန်ကုန်စံတော်ချိန် (UTC+6:30) မနက် 9:00 AM မှ ည 8:00 PM အထိ (တစ်ရက် ၁၂ ကြိမ်)
ACTIVE_HOURS_YANGON = set(range(9, 21))

_dl_lock = asyncio.Lock()


def get_yangon_now():
    return datetime.now(timezone.utc) + timedelta(hours=6, minutes=30)


async def fetch_next_task():
    if not SYNC_SECRET:
        LOGGER.error("AUTO-DL: SYNC_SECRET is not set in Variables")
        return None
    url = f"{CHPROTECT_URL}/api/next-dl-task"
    headers = {"x-sync-secret": SYNC_SECRET, "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json={}, timeout=20) as resp:
                if resp.status != 200:
                    LOGGER.error(f"AUTO-DL: Failed to fetch task HTTP {resp.status}")
                    return None
                return await resp.json()
    except Exception as e:
        LOGGER.error(f"AUTO-DL: Error fetching task: {e}")
        return None


async def mark_task_done(link_id=None, provider_album_id=None, title="", status="done"):
    if not SYNC_SECRET:
        return
    url = f"{CHPROTECT_URL}/api/mark-downloaded"
    headers = {"x-sync-secret": SYNC_SECRET, "Content-Type": "application/json"}
    payload = {
        "link_id": link_id,
        "provider_album_id": provider_album_id,
        "title": title,
        "status": status
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=20) as resp:
                return await resp.json()
    except Exception as e:
        LOGGER.error(f"AUTO-DL: Error marking done: {e}")


async def find_random_album_for_artist(artist_name: str, downloaded_ids: set):
    candidates = []

    # 1. Search on Tidal first
    if bot_set.tidal and tidalapi.saved:
        try:
            res = await tidalapi.search(artist_name, types='ARTISTS,ALBUMS', limit=30)
            artists = res.get('artists', {}).get('items', [])
            matched_artist = next(
                (a for a in artists if a.get('name', '').strip().lower() == artist_name.strip().lower()),
                artists[0] if artists else None
            )

            album_items = []
            if matched_artist:
                alb_res = await tidalapi.get_artist_albums(matched_artist['id'])
                album_items = alb_res.get('items', [])
            else:
                album_items = res.get('albums', {}).get('items', [])

            for alb in album_items:
                alb_id = str(alb.get('id'))
                num_tracks = int(alb.get('numberOfTracks') or 0)
                key = f"tidal_{alb_id}"
                if num_tracks >= 2 and key not in downloaded_ids:
                    candidates.append({
                        "provider_album_id": key,
                        "url": f"https://tidal.com/browse/album/{alb_id}",
                        "title": alb.get('title', f"Tidal Album {alb_id}")
                    })
        except Exception as e:
            LOGGER.error(f"AUTO-DL Tidal search error ({artist_name}): {e}")

    # 2. Search on Qobuz if Tidal found nothing
    if not candidates and bot_set.qobuz:
        try:
            q_res = await qobuz_api.search_catalog(artist_name, search_type="albums", limit=40)
            items = q_res.get("albums", {}).get("items", [])
            for alb in items:
                alb_id = str(alb.get("id"))
                tracks_count = int(alb.get("tracks_count") or 0)
                alb_artist = alb.get("artist", {}).get("name", "").lower()
                key = f"qobuz_{alb_id}"
                if tracks_count >= 2 and key not in downloaded_ids:
                    if artist_name.lower() in alb_artist or not candidates:
                        candidates.append({
                            "provider_album_id": key,
                            "url": f"https://open.qobuz.com/album/{alb_id}",
                            "title": alb.get("title", f"Qobuz Album {alb_id}")
                        })
        except Exception as e:
            LOGGER.error(f"AUTO-DL Qobuz search error ({artist_name}): {e}")

    if not candidates:
        return None
    return random.choice(candidates)


async def execute_single_auto_job(notify_chat_id=None):
    async with _dl_lock:
        task = await fetch_next_task()
        if not task or not task.get("ok"):
            if notify_chat_id:
                await aio.send_message(notify_chat_id, "⚠️ Channel Guard Bot ဆီမှ Task ရယူ၍ မရပါ (SYNC_SECRET စစ်ဆေးပါ)။")
            return

        task_type = task.get("task_type")
        downloaded_ids = set(task.get("downloaded_ids") or [])
        admin_id = notify_chat_id or next(iter(bot_set.admins), None)

        if not admin_id:
            LOGGER.error("AUTO-DL: No ADMIN_ID configured in bot_set.admins")
            return

        bot_set.upload_mode = 'Telegram'
        bot_set.album_zip = True
        bot_set.playlist_zip = True

        target_url = None
        link_id = None
        provider_album_id = None
        album_title = ""

        if task_type == "link":
            link_id = task.get("link_id")
            target_url = task.get("url")
            album_title = target_url
        elif task_type == "artist":
            artist_name = task.get("artist_name")
            picked = await find_random_album_for_artist(artist_name, downloaded_ids)
            if not picked:
                msg = f"ℹ️ Auto-DL: Artist <b>{artist_name}</b> အတွက် မဒေါင်းရသေးသော Album အသစ် မတွေ့ပါ။"
                LOGGER.info(msg)
                if notify_chat_id:
                    await aio.send_message(notify_chat_id, msg)
                return
            target_url = picked["url"]
            provider_album_id = picked["provider_album_id"]
            album_title = f"{picked['title']} ({artist_name})"
        else:
            if notify_chat_id:
                await aio.send_message(notify_chat_id, "ℹ️ Queue ထဲတွင် Pending Link နှင့် Artist မရှိသေးပါ။")
            return

        status_msg = await aio.send_message(
            admin_id,
            f"🤖 <b>Auto-Downloader စတင်နေပါသည်...</b>\n• Target: <code>{album_title}</code>\n• Link: {target_url}",
            disable_web_page_preview=True
        )

        fake_user = user_details.copy()
        fake_user['user_id'] = admin_id
        fake_user['name'] = "AutoBot"
        fake_user['user_name'] = "AutoBot"
        fake_user['r_id'] = status_msg.id
        fake_user['chat_id'] = admin_id
        fake_user['bot_msg'] = status_msg

        try:
            if "tidal.com" in target_url:
                await start_tidal(target_url, fake_user)
            elif "qobuz.com" in target_url:
                await start_qobuz(target_url, fake_user)
            else:
                raise Exception("Unsupported URL (Only Tidal & Qobuz supported)")

            await mark_task_done(
                link_id=link_id,
                provider_album_id=provider_album_id,
                title=album_title,
                status="done"
            )
            LOGGER.info(f"AUTO-DL: Completed {album_title}")
        except Exception as e:
            LOGGER.error(f"AUTO-DL Failed ({target_url}): {e}")
            await mark_task_done(link_id=link_id, status="failed")
            await aio.send_message(admin_id, f"❌ <b>Auto-DL Error:</b> {e}")


async def hourly_auto_loop():
    await asyncio.sleep(15)
    LOGGER.info("AUTO-DL: Hourly scheduler started (Yangon 09:00 - 20:00, 12 albums/day)")
    last_run_slot = None
    while True:
        try:
            ygn = get_yangon_now()
            slot_key = f"{ygn.strftime('%Y-%m-%d')}_{ygn.hour}"
            if ygn.hour in ACTIVE_HOURS_YANGON and ygn.minute == 0 and last_run_slot != slot_key:
                last_run_slot = slot_key
                await execute_single_auto_job()
        except Exception as e:
            LOGGER.error(f"AUTO-DL Loop Error: {e}")
        await asyncio.sleep(25)


@Client.on_message(filters.command(["autonow"]))
async def cmd_auto_now(c, msg: Message):
    if not await check_user(msg=msg, restricted=True):
        return
    await execute_single_auto_job(notify_chat_id=msg.chat.id)
