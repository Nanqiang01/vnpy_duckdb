from datetime import datetime

import duckdb
import polars as pl
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    TickOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS

db: duckdb.DuckDBPyConnection = duckdb.connect(":memory:")
db.execute(f"CREATE SECRET (TYPE S3, KEY_ID '{SETTINGS["database.user"]}', SECRET '{SETTINGS["database.password"]}', ENDPOINT '{SETTINGS["database.host"]}:{SETTINGS["database.port"]}', URL_STYLE 'path', USE_SSL false);")
db.execute(f"CREATE SECRET (TYPE postgres, HOST '{SETTINGS["database.host"]}', PORT 5432, DATABASE ducklake_catalog, USER 'postgres', PASSWORD 'postgres');")
db.execute(f"ATTACH 'ducklake:postgres:dbname=ducklake_catalog host=localhost' AS metadata (DATA_PATH 's3://{SETTINGS["database.database"]}/');")

CREATE_BAR_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS metadata.dbbardata (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "datetime" TIMESTAMP NOT NULL,
    "interval" VARCHAR(10) NOT NULL,
    "volume" FLOAT,
    "turnover" FLOAT,
    "open_interest" FLOAT,
    "open_price" FLOAT,
    "high_price" FLOAT,
    "low_price" FLOAT,
    "close_price" FLOAT,
);
ALTER TABLE metadata.dbbardata SET PARTITIONED BY ("interval", "exchange", "symbol");
"""

CREATE_TICK_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS metadata.dbtickdata (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "datetime" TIMESTAMP NOT NULL,
    "name" VARCHAR(50),
    "volume" FLOAT,
    "turnover" FLOAT,
    "open_interest" FLOAT,
    "last_price" FLOAT,
    "last_volume" FLOAT,
    "limit_up" FLOAT,
    "limit_down" FLOAT,
    "open_price" FLOAT,
    "high_price" FLOAT,
    "low_price" FLOAT,
    "pre_close" FLOAT,
    "bid_price_1" FLOAT, "bid_price_2" FLOAT, "bid_price_3" FLOAT, "bid_price_4" FLOAT, "bid_price_5" FLOAT,
    "ask_price_1" FLOAT, "ask_price_2" FLOAT, "ask_price_3" FLOAT, "ask_price_4" FLOAT, "ask_price_5" FLOAT,
    "bid_volume_1" FLOAT, "bid_volume_2" FLOAT, "bid_volume_3" FLOAT, "bid_volume_4" FLOAT, "bid_volume_5" FLOAT,
    "ask_volume_1" FLOAT, "ask_volume_2" FLOAT, "ask_volume_3" FLOAT, "ask_volume_4" FLOAT, "ask_volume_5" FLOAT,
    "localtime" TIMESTAMP,
);
ALTER TABLE metadata.dbtickdata SET PARTITIONED BY ("exchange", "symbol");
"""

CREATE_BAROVERVIEW_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS metadata.dbbaroverview (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "interval" VARCHAR(10) NOT NULL,
    "count" INTEGER,
    "start" TIMESTAMP,
    "end" TIMESTAMP,
);
"""

CREATE_TICKOVERVIEW_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS metadata.dbtickoverview (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "count" INTEGER,
    "start" TIMESTAMP,
    "end" TIMESTAMP,
);
"""

SAVE_BAR_QUERY: str = """
MERGE INTO metadata.dbbardata
    USING df AS upserts
    ON (
        dbbardata.symbol = upserts.symbol AND
        dbbardata.exchange = upserts.exchange AND
        dbbardata.interval = upserts.interval AND
        dbbardata.datetime = upserts.datetime
    )
    WHEN NOT MATCHED THEN INSERT;
"""

SAVE_BAROVERVIEW_QUERY: str = """
MERGE INTO metadata.dbbaroverview 
    USING (
        SELECT
            $symbol AS symbol,
            $exchange AS exchange,
            $interval AS interval,
            $count AS count,
            $start AS start,
            $end AS end
    ) AS upserts
    ON (
        dbbaroverview.symbol = upserts.symbol AND
        dbbaroverview.exchange = upserts.exchange AND
        dbbaroverview.interval = upserts.interval
    )
    WHEN MATCHED THEN UPDATE
    WHEN NOT MATCHED THEN INSERT;
"""

LOAD_BAR_QUERY: str = """
SELECT * FROM metadata.dbbardata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
    AND "datetime" >= $start
    AND "datetime" <= $end
ORDER BY "datetime" ASC;
"""

LOAD_BAROVERVIEW_QUERY: str = """
SELECT * FROM metadata.dbbaroverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval;
"""

COUNT_BAR_QUERY: str = """
SELECT COUNT("close_price") FROM metadata.dbbardata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

SAVE_TICK_QUERY: str = """
MERGE INTO metadata.dbtickdata
    USING df AS upserts
    ON (
        dbtickdata.symbol = upserts.symbol AND
        dbtickdata.exchange = upserts.exchange AND
        dbtickdata.datetime = upserts.datetime
    )
    WHEN NOT MATCHED THEN INSERT;
"""

SAVE_TICKOVERVIEW_QUERY: str = """
MERGE INTO metadata.dbtickoverview 
    USING (
        SELECT
            $symbol AS symbol,
            $exchange AS exchange,
            $interval AS interval,
            $count AS count,
            $start AS start,
            $end AS end
    ) AS upserts
    ON (
        dbtickoverview.symbol = upserts.symbol AND
        dbtickoverview.exchange = upserts.exchange
    )
    WHEN MATCHED THEN UPDATE
    WHEN NOT MATCHED THEN INSERT;
"""

LOAD_TICK_QUERY: str = """
SELECT * FROM metadata.dbtickdata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "datetime" >= $start
    AND "datetime" <= $end
ORDER BY "datetime" ASC;
"""

LOAD_TICKOVERVIEW_QUERY: str = """
SELECT * FROM metadata.dbtickoverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
"""

COUNT_TICK_QUERY: str = """
SELECT COUNT("last_price") FROM metadata.dbtickdata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

DELETE_BAR_QUERY: str = """
DELETE FROM metadata.dbbardata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

DELETE_BAROVERVIEW_QUERY: str = """
DELETE FROM metadata.dbbaroverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

DELETE_TICK_QUERY: str = """
DELETE FROM metadata.dbtickdata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
"""

DELETE_TICKOVERVIEW_QUERY: str = """
DELETE FROM metadata.dbtickoverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
"""

LOAD_ALL_BAROVERVIEW_QUERY: str = """
SELECT * FROM metadata.dbbaroverview;
"""

LOAD_ALL_TICKOVERVIEW_QUERY: str = """
SELECT * FROM metadata.dbtickoverview;
"""

MERGE_FILES_QUERY: str = """
CALL ducklake_merge_adjacent_files('metadata');
"""

class DuckdbDatabase(BaseDatabase):
    """"""

    def __init__(self) -> None:
        """"""
        self.db : duckdb.DuckDBPyConnection = db
        self.cursor: duckdb.DuckDBPyConnection = self.db.cursor()
        self.cursor.execute(MERGE_FILES_QUERY)
        # 1. K线数据表
        self.cursor.execute(CREATE_BAR_TABLE_QUERY)
        # 2. Tick数据表
        self.cursor.execute(CREATE_TICK_TABLE_QUERY)
        # 3. K线汇总表
        self.cursor.execute(CREATE_BAROVERVIEW_TABLE_QUERY)
        # 4. Tick汇总表
        self.cursor.execute(CREATE_TICKOVERVIEW_TABLE_QUERY)

    def save_bar_data(self, bars: list[BarData], stream: bool = False) -> bool:
        """保存K线数据"""
        # 读取主键参数
        bar: BarData = bars[0]
        symbol: str = bar.symbol
        exchange: Exchange = bar.exchange
        interval: Interval | None = bar.interval

        # 转换为 DuckDB 友好的列表格式 (list of Tuples)
        # 顺序必须与 INSERT 语句中的列顺序一致
        bar_data: list[dict] = []

        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)

            d: dict = bar.__dict__
            d["exchange"] = d["exchange"].value
            d["interval"] = d["interval"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            bar_data.append(d)

        df: pl.DataFrame = pl.from_records(bar_data, schema=[ 
            "symbol",
            "exchange",
            "datetime",
            "interval",
            "volume",
            "turnover",
            "open_interest",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
        ]).sort(by="datetime")

        # 使用upsert操作将数据更新到数据库中
        self.db.execute(SAVE_BAR_QUERY)

        # 更新K线汇总数据
        params: dict = {
            "symbol": symbol,
            "exchange": exchange.value,
            "interval": interval.value
        }

        self.execute(LOAD_BAROVERVIEW_QUERY, params)
        row: tuple = self.cursor.fetchone()

        # New contract
        if not row:
            data: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": interval.value,
                "start": df["datetime"][0],
                "end": df["datetime"][-1],
                "count": len(bars)
            }
        # Existing contract
        else:
            self.execute(COUNT_BAR_QUERY, params)
            count = self.cursor.fetchone()[0]

            data: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": interval.value,
                "start": min(df["datetime"][0], row[4]),
                "end": max(df["datetime"][-1], row[5]),
                "count": count
            }

        self.execute(SAVE_BAROVERVIEW_QUERY, data)

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        tick_data: list[dict] = []

        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)

            d: dict = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            tick_data.append(d)

        df: pl.DataFrame = pl.from_records(tick_data, schema=[ 
            "symbol",
            "exchange",
            "datetime",
            "name",
            "volume",
            "turnover",
            "open_interest",
            "last_price",
            "last_volume",
            "limit_up",
            "limit_down",
            "open_price", "high_price", "low_price", "pre_close",
            "bid_price_1", "bid_price_2", "bid_price_3", "bid_price_4", "bid_price_5",
            "ask_price_1", "ask_price_2", "ask_price_3", "ask_price_4", "ask_price_5",
            "bid_volume_1", "bid_volume_2", "bid_volume_3", "bid_volume_4", "bid_volume_5",
            "ask_volume_1", "ask_volume_2", "ask_volume_3", "ask_volume_4", "ask_volume_5",
            "localtime",
        ]).sort(by="datetime")

        # 使用upsert操作将数据更新到数据库中
        self.db.execute(SAVE_TICK_QUERY)

        # 更新Tick汇总数据
        params: dict = {
            "symbol": symbol,
            "exchange": exchange.value,
        }

        self.execute(LOAD_TICKOVERVIEW_QUERY, params)
        row: tuple = self.cursor.fetchone()

        # New contract
        if not row:
            data: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "start": df["datetime"][0],
                "end": df["datetime"][-1],
                "count": len(ticks)
            }
        # Existing contract
        else:
            self.execute(COUNT_TICK_QUERY, params)
            count = self.cursor.fetchone()[0]

            data: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "start": min(df["datetime"][0], row[4]),
                "end": max(df["datetime"][-1], row[5]),
                "count": count
            }

        self.execute(SAVE_TICKOVERVIEW_QUERY, data)

        return True

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """读取K线数据"""
        params = {
            "symbol": symbol,
            "exchange": exchange.value,
            "interval": interval.value,
            "start": str(start),
            "end": str(end)
        }

        self.execute(LOAD_BAR_QUERY, params)
        data: list[tuple] = self.cursor.fetchall()

        bars: list[BarData] = []
        
        for row in data:
            bar = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=datetime.fromtimestamp(row[2].timestamp(), DB_TZ),
                interval=interval,
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                open_price=row[7],
                high_price=row[8],
                low_price=row[9],
                close_price=row[10],
                gateway_name="DB"
            )
            bars.append(bar)

        return bars

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> list[TickData]:
        """读取TICK数据"""
        params = {
            "symbol": symbol,
            "exchange": exchange.value,
            "start": str(start),
            "end": str(end)
        }
        
        self.execute(LOAD_TICK_QUERY, params)
        data: list[tuple] = self.cursor.fetchall()

        ticks: list[TickData] = []
        for row in data:
            tick = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=datetime.fromtimestamp(row[2].timestamp(), DB_TZ),
                name=row[3],
                volume=row[4],
                turnover=row[5],
                open_interest=row[6],
                last_price=row[7],
                last_volume=row[8],
                limit_up=row[9],
                limit_down=row[10],
                open_price=row[11],
                high_price=row[12],
                low_price=row[13],
                pre_close=row[14],
                bid_price_1=row[15], bid_price_2=row[16], bid_price_3=row[17], bid_price_4=row[18], bid_price_5=row[19],
                ask_price_1=row[20], ask_price_2=row[21], ask_price_3=row[22], ask_price_4=row[23], ask_price_5=row[24],
                bid_volume_1=row[25], bid_volume_2=row[26], bid_volume_3=row[27], bid_volume_4=row[28], bid_volume_5=row[29],
                ask_volume_1=row[30], ask_volume_2=row[31], ask_volume_3=row[32], ask_volume_4=row[33], ask_volume_5=row[34],
                localtime=row[35],
                gateway_name="DB"
            )
            ticks.append(tick)

        return ticks

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        params: dict = {
            "symbol": symbol,
            "exchange": exchange.value,
            "interval": interval.value,
        }

        self.execute(COUNT_BAR_QUERY, params)
        count = self.cursor.fetchone()[0]

        self.execute(DELETE_BAR_QUERY, params)

        self.cursor.execute(DELETE_BAROVERVIEW_QUERY, params)

        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        params: dict = {
            "symbol": symbol,
            "exchange": exchange.value,
        }

        self.execute(COUNT_TICK_QUERY, params)
        count = self.cursor.fetchone()[0]

        self.execute(DELETE_TICK_QUERY, params)

        self.cursor.execute(DELETE_TICKOVERVIEW_QUERY, params)

        return count

    def get_bar_overview(self) -> list[BarOverview]:
        """查询数据库中的K线汇总信息"""
        self.execute(LOAD_ALL_BAROVERVIEW_QUERY)
        data: list[tuple] = self.cursor.fetchall()

        overviews: list[BarOverview] = []

        for row in data:
            overview = BarOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                interval=Interval(row[2]),
                count=row[3],
                start=row[4],
                end=row[5],
            )
            overviews.append(overview)

        return overviews

    def get_tick_overview(self) -> list[TickOverview]:
        """查询数据库中的Tick汇总信息"""
        self.execute(LOAD_ALL_TICKOVERVIEW_QUERY)
        data: list[tuple] = self.cursor.fetchall()

        overviews: list[TickOverview] = []

        for row in data:
            overview = BarOverview(
                symbol=row[0],
                exchange=Exchange(row[1]),
                count=row[2],
                start=row[3],
                end=row[4],
            )
            overviews.append(overview)

        return overviews



    def execute(self, query: str, data: object = None) -> None:
        """Execute SQL query"""
        self.cursor.execute(query, data)
        self.db.commit()
