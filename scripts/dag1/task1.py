import requests
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import urllib3

# Отключаем предупреждение о небезопасном HTTPS
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Данные прокси ---
proxy_host = "209.127.25.129"
proxy_port = "8000"
proxy_user = "ssb3FS"
proxy_pass = "H1Xnh9"

proxies = {
    'http': f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}',
    'https': f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}',
}

headers = {
    'Host': 'api.cryptorank.io',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0',
    'Accept': '*/*',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Referer': 'https://cryptorank.io/',
    'Content-Type': 'application/json',
    'Origin': 'https://cryptorank.io/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Priority': 'u=4',
}

url = 'https://api.cryptorank.io/v0/coins/historical-prices?keys=bitcoin,ethereum,ripple,tether,bnb,solana,usdcoin,dogecoin,lido-staked-ether,tron,cardano'

# --- Настройка SQLAlchemy ---
# Формируем URL подключения к БД
# Формат: dialect+driver://username:password@host:port/database
DATABASE_URL = "postgresql://postgres:@77.221.140.63:5432/test1"

# Создаем движок SQLAlchemy
engine = create_engine(DATABASE_URL, echo=False) # echo=True для логгирования SQL запросов

# Создаем базовый класс для моделей
Base = declarative_base()

# Создаем сессию
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Определение модели таблицы ---
class CryptoPrice(Base):
    __tablename__ = "crypto_prices"

    id = Column(Integer, primary_key=True, index=True) # Добавлено поле id
    token_name = Column(String, index=True)
    currency = Column(String, index=True)
    period = Column(String, index=True)
    price = Column(Float)
    volatility = Column(Float, nullable=True) # Добавлено поле volatility
    updated_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CryptoPrice(token_name='{self.token_name}', currency='{self.currency}', period='{self.period}', price={self.price})>"

# --- Создание таблицы (если её ещё нет) ---
# Это создаст таблицу в БД, если она не существует, основываясь на модели выше.
# В production среде лучше использовать миграции (например, Alembic).
Base.metadata.create_all(bind=engine)

# --- Основная логика ---
def main():
    # Создаем сессию
    db = SessionLocal()
    try:
        print("✅ Успешно подключено к БД через SQLAlchemy")

        # --- Запрос к API ---
        response = requests.get(url, headers=headers, verify=False, timeout=30, proxies=proxies)

        if response.status_code == 200:
            data = response.json()

            if 'data' in data:
                objects_to_insert = []

                for token_name, token_data in data['data'].items():
                    # Получаем волатильность
                    volatility_data = token_data.get('volatility', {})
                    volatility_usd = volatility_data.get('USD')
                    volatility_btc = volatility_data.get('BTC')
                    volatility_eth = volatility_data.get('ETH')

                    for period, prices in token_data.get('histPrices', {}).items():
                        usd_price = prices.get('USD')
                        btc_price = prices.get('BTC')
                        eth_price = prices.get('ETH')

                        # Создаем объекты модели для каждой записи
                        if usd_price is not None:
                            objects_to_insert.append(CryptoPrice(
                                token_name=token_name,
                                currency='USD',
                                period=period,
                                price=usd_price,
                                volatility=volatility_usd
                            ))
                        if btc_price is not None:
                            objects_to_insert.append(CryptoPrice(
                                token_name=token_name,
                                currency='BTC',
                                period=period,
                                price=btc_price,
                                volatility=volatility_btc
                            ))
                        if eth_price is not None:
                            objects_to_insert.append(CryptoPrice(
                                token_name=token_name,
                                currency='ETH',
                                period=period,
                                price=eth_price,
                                volatility=volatility_eth
                            ))

                #_bulk_save_objects или bulk_insert_mappings могут быть эффективнее,
                # но add_all проще для начала
                if objects_to_insert:
                    db.add_all(objects_to_insert)
                    db.commit()
                    print(f"✅ Успешно записано {len(objects_to_insert)} строк в БД через SQLAlchemy.")
                else:
                    print("🟡 Нет данных для вставки.")
        # изменение 2
            else:
                print("❌ Ключ 'data' не найден в ответе.")
                print("Ответ:", response.text[:500])

        else:
            print(f"❌ Ошибка запроса: {response.status_code}")
            print("Ответ:", response.text[:500])

    except json.JSONDecodeError as e:
        print(f"❌ Ошибка парсинга JSON: {e}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка запроса: {e}")
    except Exception as e: # Перехватываем общие исключения SQLAlchemy
        print(f"❌ Ошибка SQLAlchemy: {e}")
        db.rollback()
    finally:
        db.close() # Закрываем сессию

if __name__ == "__main__":
    main()