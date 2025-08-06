import requests
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import urllib3
import argparse

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser()
parser.add_argument("--host", dest="host")
parser.add_argument("--dbname", dest="dbname")
parser.add_argument("--user", dest="user")
parser.add_argument("--jdbc_password", dest="jdbc_password")
parser.add_argument("--port", dest="port", default='5432')
args = parser.parse_args()

print('host =', args.host)
print('dbname =', args.dbname)
print('user =', args.user)
print('jdbc_password =', args.jdbc_password)
print('port =', args.port)

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

DATABASE_URL = f"postgresql://{args.user}:{args.jdbc_password}@{args.host}:{args.port}/{args.dbname}"

engine = create_engine(DATABASE_URL, echo=False)
Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class CryptoPrice2(Base):
    __tablename__ = "crypto_prices_2"

    id = Column(Integer, primary_key=True, index=True)
    token_name = Column(String, index=True)
    currency = Column(String, index=True)
    period = Column(String, index=True)
    price = Column(Float)
    volatility = Column(Float, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<CryptoPrice2(token_name='{self.token_name}', currency='{self.currency}', period='{self.period}', price={self.price})>"

Base.metadata.create_all(bind=engine)

def main():
    db = SessionLocal()
    try:
        print("✅ Connected to DB")

        response = requests.get(url, headers=headers, verify=False, timeout=30, proxies=proxies)

        if response.status_code == 200:
            data = response.json()

            if 'data' in data:
                objects_to_insert = []

                for token_name, token_data in data['data'].items():
                    volatility_data = token_data.get('volatility', {})
                    volatility_usd = volatility_data.get('USD')
                    volatility_btc = volatility_data.get('BTC')
                    volatility_eth = volatility_data.get('ETH')

                    for period, prices in token_data.get('histPrices', {}).items():
                        usd_price = prices.get('USD')
                        btc_price = prices.get('BTC')
                        eth_price = prices.get('ETH')

                        if usd_price is not None:
                            objects_to_insert.append(CryptoPrice2(
                                token_name=token_name,
                                currency='USD',
                                period=period,
                                price=usd_price,
                                volatility=volatility_usd
                            ))
                        if btc_price is not None:
                            objects_to_insert.append(CryptoPrice2(
                                token_name=token_name,
                                currency='BTC',
                                period=period,
                                price=btc_price,
                                volatility=volatility_btc
                            ))
                        if eth_price is not None:
                            objects_to_insert.append(CryptoPrice2(
                                token_name=token_name,
                                currency='ETH',
                                period=period,
                                price=eth_price,
                                volatility=volatility_eth
                            ))

                if objects_to_insert:
                    db.add_all(objects_to_insert)
                    db.commit()
                    print(f"✅ Inserted {len(objects_to_insert)} rows into crypto_prices_2")
                else:
                    print("🟡 No data to insert")

            else:
                print("❌ Key 'data' not found in response")
                print("Response snippet:", response.text[:500])

        else:
            print(f"❌ Request error: {response.status_code}")
            print("Response snippet:", response.text[:500])

    except Exception as e:
        print(f"❌ Exception: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    main()
