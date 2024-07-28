import requests
import asyncio
import aiohttp

from time import sleep
from pathlib import Path

from pytonlib import TonlibClient
from pytonlib.utils.tlb import Transaction, Slice, deserialize_boc
from tonsdk.utils import b64str_to_bytes


mainnet_config_url = 'https://ton.org/global.config.json'
testnet_config_url = 'https://ton.org/testnet-global.config.json'
stonfi_api_url = 'https://api.ston.fi/'
tonapi_url = 'https://tonapi.io/v2/'

# downloading config
config = requests.get(mainnet_config_url).json()

# create keystore directory for tonlib
keystore_dir = '/tmp/ton_keystore'
Path(keystore_dir).mkdir(parents=True, exist_ok=True)


# Функция использует другую библиотеку и выводит статус транзакций. Почему-то метод get_token_data не работает!
async def func():
    client = TonlibClient(ls_index=2,
                          config=config,
                          keystore=keystore_dir,
                          tonlib_timeout=60)
    await client.init()

    a = await client.get_transactions(account='EQAz9itr_mkgt9ED3QUe30ZP7hx0WMcvtLEFL2CNh6Dr05dS', limit=10)
    for tr in a:
        cell = deserialize_boc(b64str_to_bytes(tr['data']))
        tr_data = Transaction(Slice(cell))
        print(tr_data.description.action.result_code)

    print(await client.get_token_data(address='EQAz9itr_mkgt9ED3QUe30ZP7hx0WMcvtLEFL2CNh6Dr05dS'))


# Функция выводит распарсенные блоки masterchain через http-запросы на tonapi url
async def chiiks(seqno_start=39314930, seqno_stop=39314937):
    async with aiohttp.ClientSession() as session:
        for masterchain_seqno in range(seqno_start, seqno_stop):
            sleep(0.25)  # эмпирически подобрано
            res = await session.get(url=tonapi_url + f'blockchain/masterchain/{masterchain_seqno}/transactions')
            data = await res.json()
            data = data['transactions']
            for a in data:
                a = a['hash']
                print(a)
            print('\n\n')


# Функция выводит второе значение ключа interface транзакции
async def tr_interface(tr_hash="0:1003cbe0950a66fbdb070b5f1bbb49e808e7ff222ded595eea30ce6a13d3a5d7"):
    async with aiohttp.ClientSession() as session:
        res = await session.get(
            url=tonapi_url + f'accounts/{tr_hash}')
        data = await res.json()
        print(data['interfaces'][1], '\n\n')


# Функция выводит хэши транзакций одного блока
async def block_trans(block_id='(0,4000000000000000,41719248)'):
    async with aiohttp.ClientSession() as session:
        workchain_tr = await session.get(
            url=tonapi_url + f'blockchain/blocks/{block_id}/transactions')
        data = await workchain_tr.json()
        data = data['transactions']
        for tr in data:
            tr_hash = tr['hash']
            print(tr_hash)


# Способ напрямую парсить блок и получать информацию о том, относится ли транзакция к какому-либо пулу на Stonfi
# Проблема в скорости: чаще 2 rps сайт tonapi банит. Решения: покупать доступ к платной апи или использовать
# другие библиотеки (более низкоуровневые)
async def find_trs_on_stonfi_pool():
    async with aiohttp.ClientSession() as session:
        workchain_tr = await session.get(
            url=tonapi_url + f'blockchain/blocks/(0,4000000000000000,41719248)/transactions')

        block_data = await workchain_tr.json()
        transactions_data = block_data['transactions']

        for tr in transactions_data:
            tr_hash = tr['hash']
            print(tr_hash)
            sleep(0.5)

            transaction = await session.get(url=tonapi_url + f'blockchain/transactions/{tr_hash}')
            tr_data = await transaction.json()

            if tr_data['success']:
                acc_add = tr_data['account']['address']
                acc = await session.get(url=tonapi_url + f'accounts/{acc_add}')
                acc_data = await acc.json()
                try:
                    interface = acc_data['interfaces']
                    for inter in interface:
                        if inter == 'stonfi_pool':
                            print('\n\n', f'-------Транзакция {tr_hash} относится к пулу Stonfi-------', '\n\n')
                except:
                    pass



# Использование хитрости: применение API декса Stonfi с вытягиванием всех пулов ликвидности.
# Функция выводит адресс пула, символы монет и номер блока с транзакцией создания пула. Проблема та же –
# rps к tonapi. Нужно решение
async def pools_stonfi_api():
    async with aiohttp.ClientSession() as session:
        pools_data = requests.get(url=stonfi_api_url + 'v1/pools').json()
        pools = pools_data['pool_list']
        print(len(pools))
        print(pools[0])

        for pool in pools:
            if float(pool['lp_total_supply_usd']) > 1:
                sleep(0.6) #подобрано эмпирически
                pool_add = pool['address']
                
                token0_symbol = token1_symbol = '####'
                token0_add = pool['token0_address']
                token0_info_data = await session.get(url=tonapi_url + f'jettons/{token0_add}')
                token0_info = await token0_info_data.json()
                try:
                    token0_symbol = token0_info['metadata']['symbol']
                except:
                    pass

                if token0_symbol != '####':
                    token1_add = pool['token1_address']
                    token1_info_data = await session.get(url=tonapi_url + f'jettons/{token1_add}')
                    token1_info = await token1_info_data.json()
                    try:
                        token1_symbol = token1_info['metadata']['symbol']
                    except:
                        pass

                    if token1_symbol != '####':
                        print('pool_address:', pool_add)
                        print(token0_symbol, '-', token1_symbol)

                        pool_trs_data = await session.get(
                            url=tonapi_url + f'blockchain/accounts/{pool_add}/transactions')
                        pool_trs = await pool_trs_data.json()
                        pool_trs = pool_trs['transactions']
                        first_tr = pool_trs[-1]
                        first_tr_block = first_tr['block']
                        print('block: ', first_tr_block, '\n')


asyncio.get_event_loop().run_until_complete(pools_stonfi_api())
