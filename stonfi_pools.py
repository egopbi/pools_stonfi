import requests
import asyncio
import aiohttp
import codecs

from time import sleep
from pathlib import Path
from pytoniq import LiteClient, Contract

mainnet_config_url = 'https://ton.org/global.config.json'
testnet_config_url = 'https://ton.org/testnet-global.config.json'
stonfi_api_url = 'https://api.ston.fi/'
tonapi_url = 'https://tonapi.io/v2/'

# downloading config
config = requests.get(mainnet_config_url).json()

# create keystore directory for tonlib
keystore_dir = '/tmp/ton_keystore'
Path(keystore_dir).mkdir(parents=True, exist_ok=True)


def decode_bits(bits):
    """Раскодирует строку битов в текст и в байты."""

    # Разбиваем строку битов на группы по 8 бит
    byte_groups = [bits[i:i+8] for i in range(0, len(bits), 8)]

    # Преобразуем каждую группу в десятичное число
    decimal_values = [int(group, 2) for group in byte_groups]

    # Преобразуем десятичные значения в байты
    bytes_data = bytes(decimal_values)

    # Декодируем байты в текст с помощью UTF-8
    textik = bytes_data.decode('utf-8')

    return textik, bytes_data


def b64str_to_bytes(b64str):
    b64bytes = codecs.encode(b64str, "utf-8")
    return codecs.decode(b64bytes, "base64")


async def masterchain_transactions(seqno_start=39314930, seqno_stop=39314937):
    """Функция выводит распарсенные блоки masterchain через http-запросы на tonapi url"""

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


async def find_trs_on_stonfi_pool():
    """Способ напрямую парсить блок и получать информацию о том, относится ли транзакция к какому-либо пулу на Stonfi
    Проблема в скорости: чаще 2 rps сайт tonapi банит. Решения: покупать доступ к платной апи или использовать
    другие библиотеки (более низкоуровневые)"""

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


def get_pools_from_stonfi_api():
    """Получаем актуальный список пулов на Ston.Fi"""

    pools_data = requests.get(url=stonfi_api_url + 'v1/pools').json()
    pools = pools_data['pool_list']
    print('Количество пулов на StonFi: ', len(pools))
    return pools


async def pools_stonfi_api():
    """Использование хитрости: применение API декса Stonfi с вытягиванием всех пулов ликвидности.
    Функция выводит адресс пула, символы монет и номер блока с транзакцией создания пула. Основная проблема –
    ограничение на 1 rps к tonapi. Нужно решение"""

    async with aiohttp.ClientSession() as session:
        pools = get_pools_from_stonfi_api()
        for pool in pools:
            if float(pool['lp_total_supply_usd']) > 1:
                sleep(0.6) #подобрано эмпирически
                pool_add = pool['address']
                print(f'------______--------____{pool_add}')
                
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
                        print(len(pool_trs))
                        # Выводит только последние 100 транзакций, надо решать
                        first_tr = pool_trs[-1]
                        first_tr_block = first_tr['block']
                        print('block: ', first_tr_block, '\n')


async def get_token_symbol_from_address(token_add, client: LiteClient):#, problem_tokens=problem_tokens):
    """Получает символ токена по адресу его Jetton-wallet"""           # для кэширования проблемных токенов

    token_contract_wallet = await Contract.from_address(provider=client, address=token_add)
    token_contract_data = await token_contract_wallet.run_get_method('get_wallet_data')
    token_jet_master = token_contract_data[2]
    token_jet_master_add = token_jet_master.load_address()

    if token_jet_master_add.to_str() == 'EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs':
        token_sym = 'USD₮'
    elif token_jet_master_add.to_str() == 'EQCM3B12QK1e4yZSf8GtBRT0aLMNyEsBc_DhVfRRtOEffLez':
        token_sym = 'pTON'
    elif token_jet_master_add.to_str() == 'EQBlU_tKISgpepeMFT9t3xTDeiVmo25dW_4vUOl6jId_BNIj':
        token_sym = 'KOTE'
    elif token_jet_master_add.to_str() == 'EQB6BMDGIv7P_Ppb-IuWWC6twwigPEz2CS9gov4mc-Lkfh1H':
        token_sym = 'Bnot'
    else:
        token_jet_contract = await Contract.from_address(provider=client, address=token_jet_master_add)
        token_jet_data = await token_jet_contract.run_get_method('get_jetton_data')
        token_sym = '####'

        try: # metadata onchain
            token_metadata_sym = list(token_jet_data[3]
                                  .begin_parse()
                                  .load_ref()
                                  .begin_parse()
                                  .load_hashmap(256)
                                  .values()
                                  )[2]  # Slice с метадатой

            token_metadata = token_metadata_sym.load_ref().begin_parse().load_string()

            token_sym = token_metadata[1:]
            print(f'metadata {token_sym} is onchain')
        except:
            pass

        try: #metadata offchain: # ToDo НЕ РАБОТАЕТ, НАДО ПРОВЕРЯТЬ
            token_metadata_sym = list(token_jet_data[3]
                                       .begin_parse()
                                       .load_ref()
                                       .begin_parse()
                                       .load_hashmap(256)
                                       .values()
                                       )[1]  # Slice с метадатой
            token_metadata = token_metadata_sym.load_ref().begin_parse().load_string()
            token_metadata_json = token_metadata[1:]
            token_sym = requests.get(token_metadata_json).json()['symbol']
            print(f'metadata {token_sym} is offchain')
        except:
            pass

        if token_sym == '####':
            print('We have a trouble with metadata token ', token_jet_master_add)
            # problem_tokens[token_jet_master_add] += 1
            # ToDo закэшировать проблемные токены и выяснить в чем дело
    return token_sym

async def pytoniq_pools():
    # with open('problem_tokens.txt', 'a') as file:
        # ToDo закэшировать проблемные токены и выяснить в чем дело
        async with LiteClient.from_mainnet_config(ls_i=2, trust_level=2) as client:

            pools = get_pools_from_stonfi_api()

            # ToDo сделать каждый запрос независмым для увеличения скорости работы кода
            for pool in pools:
                if float(pool['lp_total_supply_usd']) > 1:
                    pool_add = pool['address']
                    print(f'\n Pool adress: {pool_add}')
                    #  EQBx_ASjqanfK-L_Msg_tebNEdFpkpDsl9214qgWQnZj6uVm  –  pool jUSDC-USDT

                    trs = await client.get_transactions(address=pool_add, count=500)
                    tr_first = trs[-1] # 0 - 48146843000011
                    tr_lt = tr_first.lt # 0:71fc04a3a9a9df2be2ff32c83fb5e6cd11d1699290ec97ddb5e2a816427663ea

                    block = await client.lookup_block(wc=0, shard=4000000000000000, lt=tr_lt)
                    block = block[0]              #wtf почему block.shard не соответствует реальному
                    block_info = (block.workchain, 4000000000000000, block.seqno)
                    print('Block of creation:', block_info)

                    cnt = await Contract.from_address(provider=client, address=pool_add)
                    pool_data = await cnt.run_get_method('get_pool_data')

                    token0_slice = pool_data[2]
                    token0_add = token0_slice.load_address()
                    token1_slice = pool_data[3]
                    token1_add = token1_slice.load_address()

                    token0_sym = await get_token_symbol_from_address(token0_add, client)
                    token1_sym = await get_token_symbol_from_address(token1_add, client)
                    if token0_sym != '####' and token1_sym != '####':
                        print('Pool is', token0_sym, ' – ', token1_sym)


asyncio.run(pytoniq_pools())
