import asyncio
import json
import os
import random
import re
import sys
from datetime import datetime
from time import time
from urllib.parse import unquote

import aiohttp
import pytz
from aiocfscrape import CloudflareScraper
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered
from pyrogram.raw import types
from pyrogram.raw.functions.messages import RequestAppWebView

from bot.config import settings
from bot.core.helper import format_duration
from bot.exceptions import InvalidSession
from bot.utils import logger
from bot.utils.logger import SelfTGClient
from .headers import headers

wib = pytz.timezone('Europe/Kyiv')

url_end_point = "https://tgapi.sleepagotchi.com/v1/tg"
user_data = f"{url_end_point}/getUserData?"
all_heroes = f"{url_end_point}/getAllHeroes?"
spend_gacha = f"{url_end_point}/spendGacha?"
get_clan = f"{url_end_point}/getClan?"
get_shop = f"{url_end_point}/getShop?"
buy_shop = f"{url_end_point}/buyShop?"
get_constellations = f"{url_end_point}/getConstellations?"
send_to_challenge = f"{url_end_point}/sendToChallenge?"
send_to_clan_challenge = f"{url_end_point}/sendToClanChallenge?"
level_up_hero = f"{url_end_point}/levelUpHero?"
get_daily_rewards = f"{url_end_point}/getDailyRewards?"
claim_daily_rewards = f"{url_end_point}/claimDailyRewards?"
star_up_hero = f"{url_end_point}/starUpHero?"
claim_challenges_rewards = f"{url_end_point}/claimChallengesRewards?"

self_tg_client = SelfTGClient()


class Tapper:
    def __init__(self, tg_client: Client):
        self.player = None
        self.min_index_file = "min_index.json"
        self.next_unlock_time = None
        self.session_name = tg_client.name
        self.tg_client = tg_client
        self.user_id = 0
        self.username = None
        self.first_name = None
        self.last_name = None
        self.fullname = None
        self.start_param = None
        self.wait_time = 0
        self.peer = None
        self.first_run = None
        self.game_service_is_unavailable = False
        self.already_joined_squad_channel = None
        self.user = None
        self.updated_pixels = {}
        self.socket = None
        self.socket_task = None
        self.current_user_balance = 0
        self.access_token_created_time = time()
        self.token_live_time = random.randint(500, 900)
        self.chat_instance = None
        self.user_info = None
        self.status = None

    @staticmethod
    def check_timeout_error(error):
        try:
            error_message = str(error)
            is_timeout_error = re.search("504, message='Gateway Timeout'", error_message)
            return is_timeout_error
        except Exception as e:
            return False

    def load_min_index(self):
        """
        Загружает стартовый индекс для конкретного аккаунта.
        """
        if os.path.exists(self.min_index_file):
            with open(self.min_index_file, "r") as file:
                data = json.load(file)
                return data.get(str(self.session_name), {}).get("min_index", 0)
        return 0

    def save_min_index(self, index):
        """
        Сохраняет стартовый индекс для конкретного аккаунта.
        """
        data = {}
        if os.path.exists(self.min_index_file):
            with open(self.min_index_file, "r") as file:
                data = json.load(file)

        # Обновляем индекс для указанного аккаунта
        data[str(self.session_name)] = {"min_index": index}

        with open(self.min_index_file, "w") as file:
            json.dump(data, file)

    @staticmethod
    def check_error(error, message):
        try:
            error_message = str(error)
            is_equal = re.search(message, error_message)
            return is_equal
        except Exception as e:
            return False

    async def get_tg_web_data(self) -> str:
        try:
            with_tg = True

            if not self.tg_client.is_connected:
                with_tg = False
                try:
                    await self.tg_client.connect()
                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            if settings.USE_REF == True and settings.REF_ID:
                ref_id = settings.REF_ID
            else:
                ref_id = '72633a323431393637393935'

            self.start_param = random.choices([ref_id, '72633a323431393637393935'], weights=[50, 50])[0]

            peer = await self.tg_client.resolve_peer('sleepagotchiLITE_bot')
            input_bot_app = types.InputBotAppShortName(bot_id=peer, short_name="game")

            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                app=input_bot_app,
                platform='android',
                write_allowed=True,
                start_param=self.start_param
            ))

            auth_url = web_view.url

            tg_web_data = unquote(
                string=auth_url.split('tgWebAppData=', maxsplit=1)[1].split('&tgWebAppVersion', maxsplit=1)[0])

            self.chat_instance = re.findall(r'chat_instance=([^&]+)', tg_web_data)[0]

            try:
                if self.user_id == 0:
                    information = await self.tg_client.get_me()
                    self.user_id = information.id
                    self.first_name = information.first_name or ''
                    self.last_name = information.last_name or ''
                    self.username = information.username or ''
            except Exception as e:
                print(e)

            if with_tg is False:
                await self.tg_client.disconnect()

            return tg_web_data

        except InvalidSession as error:
            logger.error(f"Session error during Authorization: <light-yellow>{error}</light-yellow>")
            await asyncio.sleep(delay=10)

        except Exception as error:
            logger.error(
                f"Unknown error during Authorization: <light-yellow>{error}</light-yellow>")
            await asyncio.sleep(delay=random.randint(3, 8))

    async def user_data(self, http_client: aiohttp.ClientSession, query, show_error_message: bool):
        err = None
        first = True
        url = f"{user_data}{query}"
        for _ in range(3):
            try:
                response = await http_client.get(url)
                response.raise_for_status()
                data = await response.json()
                err = None
                return data
            except Exception as error:
                if first:
                    first = False
                    logger.info(f"First get user info request not always successful, retrying..")
                await asyncio.sleep(delay=random.randint(3, 6))
                err = error
                continue

        if err is not None and show_error_message == True:
            if self.check_timeout_error(err) or self.check_error(err, "Service Unavailable"):
                logger.warning(
                    f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not response.")
            else:
                logger.error(f"Unknown error during getting user info: <light-yellow>{err}</light-yellow>")
            return None

    async def find_start_index(self, http_client, query, constellations_last_index):
        """
        Поиск первого индекса, где хотя бы одно испытание не завершено.
        """
        constellations = await self.get_constellations(http_client, query, start_index=0,
                                                       amount=constellations_last_index)
        if constellations.get("status") == "success":
            for constellation in constellations["data"]["constellations"]:
                index = constellation.get("index")
                challenges = constellation.get("challenges", [])

                # Если хотя бы одно испытание не завершено
                if any(c["received"] < c["value"] for c in challenges):
                    self.save_min_index(index)
                    logger.info(f"Стартовый индекс для аккаунта {self.session_name} установлен: {index}")
                    return index

        # Если все испытания завершены
        self.save_min_index(constellations_last_index + 1)
        logger.info(f"Все испытания завершены для аккаунта {self.session_name}.")
        return constellations_last_index + 1

    async def get_start_index(self, http_client, query, constellations_last_index):
        """
        Получение стартового индекса для конкретного аккаунта: из файла или через поиск.
        """
        saved_index = self.load_min_index()

        if saved_index <= constellations_last_index:
            logger.info(f"Используем сохранённый индекс для аккаунта {self.session_name}: {saved_index}")
            return saved_index
        else:
            logger.info("Сохранённый индекс некорректен. Запускаем поиск актуального индекса...")
            return await self.find_start_index(http_client, query, constellations_last_index)

    async def spend_gacha(self, http_client: aiohttp.ClientSession, query, amount, strategy):
        url = str(f"{spend_gacha}{query}")
        for _ in range(3):
            try:
                response = await http_client.post(url, json={"amount": amount, "strategy": strategy})
                response.raise_for_status()
                data = await response.json()
                if 'heroCard' in data:
                    for hero in data['heroCard']:
                        logger.info(
                            f"<green>[Успех]</> Получен герой типа {hero['heroType']} в количестве {hero['amount']}"
                        )
                return {"status": "success", "data": data}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting template info: <magenta>Sleepagotchi</magenta> server is not response. Retrying..")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    if error:
                        logger.error(
                            f"Unknown error during getting template info: <light-yellow>{error}</light-yellow>")
                    else:
                        logger.error(f"Unknown error during getting template info.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None

    async def claim_daily_rewards(self, http_client: aiohttp.ClientSession, query):
        url = str(f"{claim_daily_rewards}{query}")
        for _ in range(3):
            try:
                response = await http_client.get(url)
                response.raise_for_status()
                data = await response.json()
                if 'rewards' in data:
                    rewards = data['rewards']
                    logger.success(
                        f"<green>[Успех]</green> Получена награда {rewards['rewardType']} в количестве {rewards['rewardAmount']}"
                    )
                return {"status": "success", "data": data}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Unknown error during getting user info: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None

    async def get_shop(self, http_client: aiohttp.ClientSession, query):
        url = str(f"{get_shop}{query}")
        for _ in range(3):
            try:
                response = await http_client.get(url)
                response.raise_for_status()
                data = await response.json()
                shop_items = data.get('shop', [])
                next_claim_free_slot = next(
                    (item['nextClaimAt'] for item in shop_items if item.get('slotType') == 'free'), None)
                return {"status": "success", "data": data, "next_claim_free_slot": next_claim_free_slot}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Unknown error during getting user info: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None

    async def buy_shop(self, http_client: aiohttp.ClientSession, query, slot_type):
        url = str(f"{buy_shop}{query}")
        for _ in range(3):
            try:
                response = await http_client.post(url, json={"slotType": slot_type})
                response.raise_for_status()
                return {"status": "success", "data": await response.json()}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Unknown error during getting user info: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None

    async def star_up_hero(self, http_client: aiohttp.ClientSession, query, hero_type):
        url = str(f"{star_up_hero}{query}")
        for _ in range(3):
            try:
                response = await http_client.post(url, json={"heroType": hero_type})
                response.raise_for_status()
                return {"status": "success", "data": await response.json()}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Unknown error during getting user info: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None

    async def lvl_up_hero(self, http_client: aiohttp.ClientSession, query, hero_type):
        url = str(f"{level_up_hero}{query}")
        for _ in range(3):
            try:
                await asyncio.sleep(random.uniform(1, 3))
                logger.info(f"Отправляем запрос на повышение уровня героя <green> {hero_type}</green>")
                response = await http_client.post(url, json={"heroType": hero_type,"strategy":"one"})
                response.raise_for_status()
                data = await response.json()
                return {"status": "success", "data": data}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Unknown error during getting user info: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None

    async def get_constellations(self, http_client: aiohttp.ClientSession, query, start_index, amount):
        url = str(f"{get_constellations}{query}")
        for _ in range(3):
            try:
                response = await http_client.post(url, json={"startIndex": start_index, "amount": amount})
                response.raise_for_status()
                data = await response.json()

                return {"status": "success", "data": data}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Unknown error during getting user info: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))

    async def get_clan(self, http_client: aiohttp.ClientSession, query, clan_id):
        url = str(f"{get_clan}{query}")
        for _ in range(3):
            try:
                response = await http_client.post(url, json={"clanId": clan_id})
                response.raise_for_status()
                data = await response.json()
                return {"status": "success", "data": data}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting clan info: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Unknown error during getting clan info: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))

    async def claim_challenges_rewards(self, http_client: aiohttp.ClientSession, query):
        url = str(f"{claim_challenges_rewards}{query}")
        for _ in range(3):
            try:
                response = await http_client.get(url)
                response.raise_for_status()
                return {"status": "success", "data": await response.json()}
            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting challenges rewards: <magenta>Sleepagotchi</magenta> server is not response.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(
                        f"Unknown error during getting challenges rewards: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None

    async def send_to_challenge(self, http_client: aiohttp.ClientSession, query, challenge_type, heroes):
        url = str(f"{send_to_challenge}{query}")
        payload = {
            "challengeType": challenge_type,
            "heroes": [
                {"slotId": hero["slotId"], "heroType": hero["heroType"]}
                for hero in heroes
            ]
        }
        print(payload)
        for _ in range(3):
            try:
                response = await http_client.post(url, json=payload)
                response.raise_for_status()
                data = await response.json()
                await asyncio.sleep(delay=random.randint(3, 5))
                return {"status": "success", "data": data}
            except aiohttp.ClientResponseError as e:
                if self.check_timeout_error(e) or self.check_error(e, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not responding.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Client response error during sending hero <light-yellow>{e}</light-yellow>")
                    logger.error(f"Response status: {e.status} - Response message: {e.message}")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None
            except Exception as error:
                logger.error(f"Unknown error during getting send to challenge: <light-yellow>{error}</light-yellow>")
                await asyncio.sleep(delay=random.randint(3, 5))
                return None

    async def send_to_clan_challenge(self, http_client: aiohttp.ClientSession, query, challenge_type):
        url = str(f"{send_to_clan_challenge}{query}")
        payload = {
            "challengeType": challenge_type,
            "heroes":[{"slotId":0,"heroType":"bonk"}]}
        for _ in range(3):
            try:
                response = await http_client.post(url, json=payload)
                response.raise_for_status()
                data = await response.json()
                await asyncio.sleep(delay=random.randint(3, 5))
                return {"status": "success", "data": data}
            except aiohttp.ClientResponseError as e:
                if self.check_timeout_error(e) or self.check_error(e, "Service Unavailable"):
                    logger.warning(
                        f"Warning during getting user info: <magenta>Sleepagotchi</magenta> server is not responding.")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    continue
                else:
                    logger.error(f"Client response error during sending hero <light-yellow>{e}</light-yellow>")
                    logger.error(f"Response status: {e.status} - Response message: {e.message}")
                    await asyncio.sleep(delay=random.randint(3, 5))
                    return None
            except Exception as error:
                logger.error(f"Unknown error during getting send to challenge: <light-yellow>{error}</light-yellow>")
                await asyncio.sleep(delay=random.randint(3, 5))
                return None


    async def run(self) -> None:
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(f"Bot will start in <ly>{random_delay}s</ly>")
            await asyncio.sleep(random_delay)

        access_token = None
        refresh_token = None
        login_need = True

        http_client = CloudflareScraper(headers=headers)

        self.access_token_created_time = 0
        self.token_live_time = random.randint(500, 900)
        tries_to_login = 4

        while True:
            # Очистка терминала после ожиданием
            os.system('cls' if os.name == 'nt' else 'clear')
            try:
                if time() - self.access_token_created_time >= self.token_live_time:
                    login_need = True

                if login_need:
                    self.tg_web_data = await self.get_tg_web_data()

                    self.access_token_created_time = time()
                    self.token_live_time = random.randint(500, 900)

                    if not self.first_run and self.tg_web_data:
                        logger.success("Logged in successfully")
                        self.first_run = True

                    login_need = False

                await asyncio.sleep(3)

            except Exception as error:
                if self.check_timeout_error(error) or self.check_error(error, "Service Unavailable"):
                    logger.warning(f"Warning during login: <magenta>Sleepagotchi</magenta> server is not responding.")
                    if tries_to_login > 0:
                        tries_to_login -= 1
                        logger.info(f"Login request not always successful, retrying..")
                        await asyncio.sleep(delay=random.randint(10, 40))
                    else:
                        await asyncio.sleep(delay=5)
                        break
                else:
                    logger.error(f"Unknown error during login: <light-yellow>{error}</light-yellow>")
                    await asyncio.sleep(delay=5)
                    break

            try:
                query = self.tg_web_data
                user = await self.user_data(http_client=http_client, query=query, show_error_message=True)

                self.user_info = user

                await asyncio.sleep(delay=random.randint(2, 5))

                if user is not None:

                    self.next_unlock_time = None
                    self.user = user
                    user_name = user['initData']['first_name']
                    logger.info(f"<green>Пользователь:</green> <cyan>{user_name}</cyan>")
                    challenges_rewards = await self.claim_challenges_rewards(http_client, query)
                    if challenges_rewards["status"] == "success":
                        logger.success(f"Награда за испытания успешно получена")
                    self.player = user.get('player', {})
                    meta = self.player.get('meta', {})
                    clan = self.player.get('clanInfo', {})
                    clan_id = clan.get('clanId')
                    resources = self.player.get('resources', {})
                    hero_cards = resources.get('heroCard', [])
                    hero_card_dict = {card['heroType']: card['amount'] for card in hero_cards}
                    constellations_last_index = meta.get('constellationsLastIndex', 0)

                    logger.info(f"<yellow>Ресурсы:</yellow>")
                    resource_display = {
                        'gold': ('🪙', 'yellow'),
                        'gem': ('💎', 'cyan'),
                        'greenStones': ('🟢', 'green'),
                        'purpleStones': ('🟣', 'magenta'),
                        'orb': ('🔮', 'blue'),
                        'points': ('⭐', 'white'),
                        'gacha': ('🎉', 'red'),
                    }

                    for resource, (emoji, color) in resource_display.items():
                        if resource in resources:
                            amount = resources[resource].get('amount', 0)
                            logger.info(
                                f"<{color}>{resource.capitalize()}: {emoji} {amount:,}</{color}> {emoji}")
                            if resource == 'gacha' and amount > 0:
                                logger.info(f"<red>Списание гачи: {amount} 🎉</red>")
                                await self.spend_gacha(http_client, query, amount, "gacha")

                    current_time_ms = time() * 1000
                    current_time = datetime.fromtimestamp(current_time_ms / 1000, tz=pytz.utc).astimezone(wib)
                    logger.info(f"<yellow>Текущее время:</> <cyan>{current_time.strftime('%H:%M:%S')} </>")

                    free_gacha_next_claim = meta.get('freeGachaNextClaim', 0)
                    next_gacha_claim_time = datetime.fromtimestamp(free_gacha_next_claim / 1000,
                                                                   tz=pytz.utc).astimezone(wib)

                    if current_time_ms >= free_gacha_next_claim:
                        result = await self.spend_gacha(http_client, query, 1, "free")
                        if result["status"] == "success":
                            logger.success(f"<green>Бесплатный гача получен!</>")
                        else:
                            logger.error(f"<red>Не удалось получить бесплатного гачу: {result['error']}</>")
                    else:
                        logger.info(f"<magenta>Бесплатный Гача уже получен.</>")
                        logger.info(
                            f"<yellow>Следующий бесплатный гача доступен в:</><cyan> {next_gacha_claim_time.strftime('%H:%M:%S')}</>")

                    # Проверка на получение ежедневной награды
                    next_daily_reward_available = meta.get('isNextDailyRewardAvailable', False)
                    if next_daily_reward_available:
                        result = await self.claim_daily_rewards(http_client, query)
                        if result["status"] == "success":
                            logger.success(f"<green>Ежедневная награда получена!</>")
                        else:
                            logger.error(f"<red>Не удалось получить ежедневную награду: {result['error']}</>")
                    else:
                        logger.info(f"<magenta>Ежедневная награда уже получена.</>")

                    # Проверка на бесплатную награду в магазине
                    shop_data = await self.get_shop(http_client, query)
                    shop_next_claim_at = shop_data.get('next_claim_free_slot', 0)
                    next_shop_claim_time = datetime.fromtimestamp(shop_next_claim_at / 1000,
                                                                  tz=pytz.utc).astimezone(wib)
                    if current_time_ms >= shop_next_claim_at:
                        result = await self.buy_shop(http_client, query, "free")
                        if result["status"] == "success":
                            logger.success(f"<green>Награда из магазина получена!</>")
                        else:
                            logger.error(f"<red>Не удалось получить награду из магазина: {result['error']}</>")
                    else:
                        logger.info(f"<magenta>Награда из магазина уже получена.</>")
                        logger.info(
                            f"<yellow>Следующая награда магазина станет доступна в:</> <cyan>{next_shop_claim_time.strftime('%H:%M:%S')}</>")

                    # Обрабатываем героев для улучшения звезд
                    for hero in self.player.get('heroes', []):
                        hero_type = hero['heroType']
                        cost_star = hero['costStar']

                        # Проверяем, достаточно ли карточек для улучшения звезд
                        if hero_type in hero_card_dict and hero_card_dict[hero_type] >= cost_star and hero['unlockAt'] == 0:
                            result = await self.star_up_hero(http_client, query, hero_type)
                            if result['status'] == 'success':
                                logger.success(f"Успешно повышены звёзды для <green> {hero_type}</>")
                            else:
                                logger.error(
                                    f"<red>Не удалось повысить звёзды для {hero_type}. Ошибка: {result.get('error', 'Неизвестная ошибка')}</>")
                    # Получить минимальное количество звезд и минимальный уровень
                    get_constel = await self.get_constellations(http_client, query,
                                                                start_index=constellations_last_index,
                                                                amount=1)
                    if get_constel["status"] != "success":
                        return

                    min_stars = get_constel["data"]['constellations'][0]['challenges'][0]['minStars']
                    min_level = get_constel["data"]['constellations'][0]['challenges'][0]['minLevel']

                    # Проверить каждого героя и вызвать функцию повышения уровня
                    for hero in self.player.get('heroes', []):
                        # Условие для улучшения героя
                        if (
                                hero['stars'] >= min_stars + 1 and
                                hero['rarity'] == 0 and
                                hero['costLevelGold'] <= resources.get('gold', {}).get('amount', 0) and
                                hero['costLevelGreen'] <= resources.get('greenStones', {}).get('amount',
                                                                                               0) and
                                hero['unlockAt'] == 0
                        ) or (
                                hero['stars'] >= min_stars and
                                hero['rarity'] in [1, 2, 3] and
                                hero['costLevelGold'] <= resources.get('gold', {}).get('amount', 0) and
                                hero['costLevelGreen'] <= resources.get('greenStones', {}).get('amount',
                                                                                               0) and
                                hero['unlockAt'] == 0
                        ) or (
                                hero['stars'] >= min_stars and
                                hero['rarity'] == 0 and
                                hero['level'] >= min_level - 1 and
                                hero['costLevelGold'] <= resources.get('gold', {}).get('amount', 0) and
                                hero['costLevelGreen'] <= resources.get('greenStones', {}).get('amount',
                                                                                               0) and
                                hero['unlockAt'] == 0
                        ):
                            while hero['level'] < min_level:
                                hero_lvl_up = await self.lvl_up_hero(http_client, query,
                                                                     hero_type=hero['heroType'])

                                if hero_lvl_up and hero_lvl_up.get('status') == 'success':
                                    heroes_from_response = hero_lvl_up.get('data', {})
                                    if not heroes_from_response:
                                        logger.error(
                                            f"<red>Ответ API не содержит список героев: {hero_lvl_up}</>")
                                        break

                                    # Получаем новый уровень героя
                                    new_level =  hero_lvl_up.get('data', {}).get('hero', {}).get('level', {})

                                    if new_level is not None:
                                        hero['level'] = new_level
                                        logger.success(
                                            f"Успешно улучшен <green> {hero['heroType']} до Уровня {new_level}</>"
                                        )
                                        if new_level >= min_level:
                                            break
                                    else:
                                        logger.error(
                                            f"<red>Не удалось получить новый уровень для {hero['heroType']}. "
                                            f"Ответ API: {heroes_from_response}</>"
                                        )
                                        break
                                else:
                                    logger.error(
                                        f"<red>Не удалось улучшить {hero['heroType']}. "
                                        f"Ошибка: {hero_lvl_up.get('error', 'Неизвестная ошибка')}</>"
                                    )
                                    break

                                await asyncio.sleep(delay=random.randint(2, 5))

                    # Получение информации о клане
                    await asyncio.sleep(delay=random.randint(2, 5))
                    clan_info = await self.get_clan(http_client, query, clan_id)
                    if clan_info.get("status") != "success":
                        logger.warning(f"❌ Не удалось получить данные для <red> Клана </red>. Пропускаем.")
                    else:
                        for hero in self.player.get('heroes', []):
                            if hero["unlockAt"] > int(time() * 1000) and hero['heroType'] == 'bonk' :
                                unlock_time = datetime.fromtimestamp(hero['unlockAt'] / 1000,
                                                                     tz=pytz.utc).astimezone(wib)
                                time_difference = unlock_time - current_time
                                formatted_time = format_duration(time_difference.total_seconds())
                                logger.warning(
                                    f"⏳ Герой '<yellow>{hero['name']}</>' ещё не разблокирован. "
                                    f"Разблокируется через <blue>{formatted_time}</blue>")
                            elif hero["unlockAt"] < int(time() * 1000) and hero['heroType'] == 'bonk' :
                                for constellation in clan_info.get("data", {}).get("constellations", []):
                                    challenges = constellation.get("challenges", [])
                                    logger.info(
                                        f"🧩 Найдено {len(challenges)} клановых испытаний в созвездии '{constellation.get('name')}'.")

                                    for challenge in challenges:
                                        challenge_name = challenge.get("name")

                                        if challenge["received"] < challenge["value"]:
                                            logger.info(
                                                f"⚠️ Клановое Испытание '<yellow>{challenge_name}</yellow>' не завершено. "
                                                f"Получено: <red>{challenge['received']}</red>, Необходимо: <green>{challenge['value']}</green>")

                                            if challenge["unlockAt"] > int(time() * 1000):
                                                unlock_time = datetime.fromtimestamp(challenge['unlockAt'] / 1000,
                                                                                     tz=pytz.utc).astimezone(wib)
                                                time_difference = unlock_time - current_time
                                                formatted_time = format_duration(time_difference.total_seconds())
                                                logger.warning(
                                                    f"⏳ Испытание '<yellow>{challenge_name}</yellow>' ещё не разблокировано. "
                                                    f"Разблокируется через <blue>{formatted_time}</blue>")
                                            else:
                                                sending = await self.send_to_clan_challenge(http_client, query,
                                                                                            challenge["challengeType"])

                                                if sending and sending["status"] == "success":
                                                    logger.success(
                                                        f"✅ Герой <cyan>Bonk</cyan> успешно отправлен на клановое испытание<green> '{challenge_name}'</green>.")
                                                    self.player = sending.get('data', {}).get('player', {})
                                                    break  # Завершаем метод после успешной отправки героя
                                                else:
                                                    logger.warning(
                                                        f"❌ Ошибка при отправке героя на клановое испытание '{challenge_name}'.")

                    # Получаем стартовый индекс для конкретного аккаунта (из файла или через поиск)
                    start_index = self.load_min_index()  # Передаем self.session_name
                    logger.info(
                        f"📂 Загружен стартовый индекс для аккаунта {user_name} из файла: <cyan>{start_index}</cyan>")

                    if start_index is None or start_index > constellations_last_index:
                        logger.info("<yellow>🔄 Запуск поиска актуального стартового индекса...</yellow>")
                        start_index = await self.find_start_index(http_client, query, constellations_last_index)

                    logger.info(f"🚀 Начинаем обработку созвездий с индекса: <green>{start_index}</green>")

                    constellations = await self.get_constellations(
                        http_client,
                        query,
                        start_index=start_index,
                        amount=(constellations_last_index - start_index + 5)
                    )

                    if constellations.get("status") != "success":
                        logger.warning(
                            f"❌ Не удалось получить данные для индексов <red> от {start_index} до {constellations_last_index + 5} </red>. Пропускаем.")
                    else:
                        suitable_heroes = [
                            hero for hero in self.player.get("heroes", [])
                            if hero["unlockAt"] == 0 and
                               hero["heroType"] != "bonk" and
                               hero["level"] >= constellations["data"]['constellations'][0]['challenges'][0][
                                   'minLevel'] and
                               hero["stars"] >= constellations["data"]['constellations'][0]['challenges'][0]['minStars']
                        ]
                        suitable_heroes.sort(key=lambda x: x.get("power", 0), reverse=True)
                        logger.info(f"✅ Подходящих героев: <magenta>{len(suitable_heroes)}</>")

                        suitable_challenges = []
                        for constellation in constellations.get("data", {}).get("constellations", []):
                            challenges = constellation.get("challenges", [])
                            for challenge in challenges:
                                if challenge["unlockAt"] < int(time() * 1000) and challenge["received"] < challenge[
                                    "value"]:
                                    suitable_challenges.append(challenge)

                        logger.info(f"✅ Доступных испытаний: <magenta>{len(suitable_challenges)}</>")

                        for constellation in constellations["data"]["constellations"]:
                            index = constellation.get("index")
                            challenges = constellation.get("challenges", [])
                            logger.info(
                                f"🧩 Найдено {len(challenges)} испытаний в созвездии '{constellation.get('name')}'.")

                            all_challenges_completed = True  # Флаг, показывающий, все ли испытания завершены

                            for challenge in challenges:
                                challenge_name = challenge.get("name")
                                if challenge["received"] < challenge["value"]:
                                    logger.info(f"⚠️ Испытание '<yellow>{challenge_name}</yellow>' не завершено. "
                                                f"Получено: <red>{challenge['received']}</red>, Необходимо: <green>{challenge['value']}</green>")

                                    if challenge["unlockAt"] > int(time() * 1000):
                                        unlock_time = datetime.fromtimestamp(challenge['unlockAt'] / 1000,
                                                                             tz=pytz.utc).astimezone(wib)
                                        time_difference = unlock_time - current_time
                                        formatted_time = format_duration(time_difference.total_seconds())
                                        logger.warning(
                                            f"⏳ Испытание '<yellow>{challenge_name}</yellow>' ещё не разблокировано. "
                                            f"Разблокируется через <blue>{formatted_time}</blue>")
                                        all_challenges_completed = False
                                        continue
                                    else:
                                        slots = [slot for slot in challenge.get("orderedSlots", []) if
                                                 slot["unlocked"] and slot["occupiedBy"] == "empty"]
                                        min_level = challenge["minLevel"]
                                        min_stars = challenge["minStars"]
                                        # Проверка наличия героев
                                        if not suitable_heroes:
                                            logger.warning(
                                                f"⚠️ Нет доступных героев для испытания '{challenge_name}'. Пропускаем.")
                                            continue

                                        # Устанавливаем множитель для испытания с resourceType == "points"
                                        if challenge.get("resourceType") == "points":
                                            multiplier = 9
                                        else:
                                            # В остальных случаях динамически вычисляем множитель
                                            multiplier = max(1, len(suitable_heroes) // len(suitable_challenges) if len(
                                                suitable_challenges) else 1)

                                        heroes_for_slots = []

                                        # Пытаемся заполнить `multiplier` слотов подходящими героями
                                        filled_slots_count = 0  # Счётчик успешно заполненных слотов
                                        # Инициализируем список для хранения занятых слотов
                                        occupied_slots = []
                                        # Присваиваем слот Id, если его нет
                                        for idx, slot in enumerate(slots):
                                            if 'slotId' not in slot:
                                                slot["slotId"] = idx
                                                # Далее обработка слотов
                                        for slot in slots:
                                            if filled_slots_count >= multiplier:
                                                break  # Прекращаем, если заполнили нужное количество слотов

                                            for hero in suitable_heroes:
                                                # Теперь можно безопасно использовать 'slotId'
                                                if slot["slotId"] in occupied_slots:
                                                    continue  # Пропускаем, если слот уже занят

                                                # Проверяем, если герой подходит по классу и уровням
                                                if (hero["class"] == slot["heroClass"] and
                                                        hero["stars"] >= min_stars and
                                                        hero["level"] >= min_level and
                                                        hero["unlockAt"] == 0):
                                                    logger.info(
                                                        f"🟢 Герой '{hero['heroType']}' назначен на слот '{slot['heroClass']}'. "
                                                        f"Уровень: {hero['level']}, Звёзды: {hero['stars']}"
                                                    )
                                                    heroes_for_slots.append({
                                                        "slotId": slot["slotId"],  # Используем слот Id
                                                        "heroType": hero["heroType"]
                                                    })
                                                    occupied_slots.append(
                                                        slot["slotId"])  # Добавляем слот в список занятых
                                                    hero["unlockAt"] = int(time() * 1000)  # Блокируем героя временно
                                                    filled_slots_count += 1
                                                    break  # Переходим к следующему слоту после успешного назначения героя

                                        # Отправка героев, если они есть
                                        if heroes_for_slots:
                                            sending = await self.send_to_challenge(
                                                http_client,
                                                query,
                                                challenge["challengeType"],
                                                heroes=heroes_for_slots
                                            )
                                            # Если отправка не удалась, откатываем изменения
                                            if not sending or sending["status"] != "success":
                                                logger.warning(f"❌ Ошибка при отправке героев. Откатываем изменения.")
                                                for hero in heroes_for_slots:
                                                    # Восстанавливаем unlockAt обратно в 0
                                                    hero_type = hero["heroType"]
                                                    for suitable_hero in suitable_heroes:
                                                        if suitable_hero["heroType"] == hero_type:
                                                            suitable_hero["unlockAt"] = 0  # Откатываем блокировку героя
                                                            break

                                            if sending and sending["status"] == "success":
                                                logger.success(
                                                    f"✅ Герои {len(heroes_for_slots)} успешно отправлены на испытание<green> '{challenge_name}'</>.")
                                                self.player = sending.get('data', {}).get('player', {})
                                            else:
                                                logger.warning(
                                                    f"❌ Ошибка при отправке героев на испытание '{challenge_name}'.")
                                        else:
                                            logger.warning(
                                                f"⚠️ Недостаточно подходящих героев для испытания '{challenge_name}'.")

                                        # Если хотя бы одно испытание не завершено, обновляем стартовый индекс
                                        all_challenges_completed = False
                            # Если все испытания для текущего индекса завершены, обновляем минимальный индекс
                            if all_challenges_completed and start_index == index:
                                start_index = index + 1
                                logger.info(
                                    f"🔄 Все испытания для индекса {index} завершены. Обновляем минимальный индекс для аккаунта {self.session_name} на {start_index}")
                                self.save_min_index(start_index)
                                logger.info(
                                    f"💾 Новый стартовый индекс для аккаунта {self.session_name} сохранён: <green>{start_index}</green>")

                    logger.info("<blue>🏁 Обработка созвездий завершена.</blue>")

                    # Проверяем время разблокировки героев
                    for hero in self.player.get("heroes", []):
                        if hero["unlockAt"] != 0 and hero["unlockAt"] > int(
                                time() * 1000):  # Игнорируем, если unlockAt равно 0
                            unlock_time = datetime.fromtimestamp(hero["unlockAt"] / 1000, tz=pytz.utc)
                            if self.next_unlock_time is None or self.next_unlock_time > unlock_time:
                                self.next_unlock_time = unlock_time.astimezone(wib)
                    constellations = await self.get_constellations(
                        http_client,
                        query,
                        start_index=start_index,
                        amount=(constellations_last_index - start_index + 5)
                    )
                    if constellations:
                        for constellation in constellations.get("data", {}).get("constellations", []):
                            challenges = constellation.get("challenges", [])
                            for challenge in challenges:
                                if challenge["unlockAt"] != 0 and challenge["unlockAt"] > int(time() * 1000):
                                    unlock_time = datetime.fromtimestamp(challenge["unlockAt"] / 1000, tz=pytz.utc)
                                    if self.next_unlock_time is None or self.next_unlock_time > unlock_time:
                                        self.next_unlock_time = unlock_time.astimezone(wib)

                    if self.next_unlock_time is not None:
                        next_time = min(next_gacha_claim_time, next_shop_claim_time, self.next_unlock_time)
                    else:
                        next_time = min(next_gacha_claim_time, next_shop_claim_time)

                    wait_time = (next_time - current_time).total_seconds()
                    self.wait_time = wait_time

                    if self.socket is not None:
                        try:
                            await self.socket.close()
                        except Exception as error:
                            logger.warning(
                                f"Unknown error during closing socket: <light-yellow>{error}</light-yellow>")

                    while self.wait_time > 0:
                        formatted_time = format_duration(int(self.wait_time))
                        sys.stdout.write(
                            f"\r\033[96m[ Ждём: \033[0m\033[97m {formatted_time} \033[0m\033[96m... ]\033[0m"
                        )
                        sys.stdout.flush()
                        await asyncio.sleep(1)
                        self.wait_time -= 1


            except Exception as error:
                logger.error(f"Unknown error: <light-yellow>{error}</light-yellow>")
                await asyncio.sleep(delay=random.randint(5, 10))


async def run_tapper(tg_client: Client):
    try:
        await Tapper(tg_client=tg_client).run()
    except InvalidSession:
        logger.error(f"{tg_client.name} | Invalid Session")
