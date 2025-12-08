from datetime import datetime

import duckdb
import polars as pl
from vnpy.trader.utility import get_file_path
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
db.execute(f"ATTACH 'dbname={SETTINGS["database.database"]} user={SETTINGS["database.user"]} password={SETTINGS["database.password"]} host={SETTINGS["database.host"]} port={SETTINGS["database.port"]}' AS pg (TYPE postgres);")


CREATE_BAR_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS pg.public.dbbardata (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "datetime" TIMESTAMPTZ NOT NULL,
    "interval" VARCHAR(10) NOT NULL,
    "volume" FLOAT,
    "turnover" FLOAT,
    "open_interest" FLOAT,
    "open_price" FLOAT,
    "high_price" FLOAT,
    "low_price" FLOAT,
    "close_price" FLOAT,
    PRIMARY KEY ("symbol", "exchange", "interval", "datetime"),
);
"""

CREATE_TICK_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS pg.public.dbtickdata (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "datetime" TIMESTAMPTZ NOT NULL,
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
    "localtime" TIMESTAMPTZ,
    PRIMARY KEY ("symbol", "exchange", "datetime"),
);
"""

CREATE_BAROVERVIEW_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS pg.public.dbbaroverview (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "interval" VARCHAR(10) NOT NULL,
    "count" INTEGER,
    "start" TIMESTAMPTZ,
    "end" TIMESTAMPTZ,
    PRIMARY KEY ("symbol", "exchange", "interval"),
);
"""

CREATE_TICKOVERVIEW_TABLE_QUERY: str = """
CREATE TABLE IF NOT EXISTS pg.public.dbtickoverview (
    "symbol" VARCHAR(50) NOT NULL,
    "exchange" VARCHAR(20) NOT NULL,
    "count" INTEGER,
    "start" TIMESTAMPTZ,
    "end" TIMESTAMPTZ,
    PRIMARY KEY ("symbol", "exchange"),
);
"""

SAVE_BAR_QUERY: str = """
INSERT INTO pg.public.dbbardata (
    "symbol", "exchange", "datetime", "interval", 
    "volume", "turnover", "open_interest", 
    "open_price", "high_price", "low_price", "close_price"
)
SELECT * FROM df
ON CONFLICT ("symbol", "exchange", "interval", "datetime") DO UPDATE SET
    "volume" = EXCLUDED.volume,
    "turnover" = EXCLUDED.turnover,
    "open_interest" = EXCLUDED.open_interest,
    "open_price" = EXCLUDED.open_price,
    "high_price" = EXCLUDED.high_price,
    "low_price" = EXCLUDED.low_price,
    "close_price" = EXCLUDED.close_price;
"""

SAVE_BAROVERVIEW_QUERY: str = """
INSERT INTO pg.public.dbbaroverview VALUES (
    $symbol, $exchange, $interval, $count, $start, $end
)
ON CONFLICT DO UPDATE SET
    "count" = EXCLUDED.count,
    "start" = EXCLUDED.start,
    "end" = EXCLUDED.end;
"""

LOAD_BAR_QUERY: str = """
SELECT * FROM pg.public.dbbardata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
    AND "datetime" >= $start
    AND "datetime" <= $end
ORDER BY "datetime" ASC;
"""

LOAD_BAROVERVIEW_QUERY: str = """
SELECT * FROM pg.public.dbbaroverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval;
"""

COUNT_BAR_QUERY: str = """
SELECT COUNT("close_price") FROM pg.public.dbbardata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

SAVE_TICK_QUERY: str = """
INSERT INTO pg.public.dbtickdata (
    "symbol", "exchange", "datetime", 
    "name", "volume", "turnover", "open_interest", "last_price", "last_volume", "limit_up", "limit_down",
    "open_price", "high_price", "low_price", "pre_close",
    "bid_price_1", "bid_price_2", "bid_price_3", "bid_price_4", "bid_price_5",
    "ask_price_1", "ask_price_2", "ask_price_3", "ask_price_4", "ask_price_5",
    "bid_volume_1", "bid_volume_2", "bid_volume_3", "bid_volume_4", "bid_volume_5",
    "ask_volume_1", "ask_volume_2", "ask_volume_3", "ask_volume_4", "ask_volume_5",
    "localtime"
)
SELECT * FROM df
ON CONFLICT ("symbol", "exchange", "datetime") DO UPDATE SET
    "name" = EXCLUDED.name,
    "volume" = EXCLUDED.volume,
    "turnover" = EXCLUDED.turnover,
    "open_interest" = EXCLUDED.open_interest,
    "last_price" = EXCLUDED.last_price,
    "last_volume" = EXCLUDED.last_volume,
    "limit_up" = EXCLUDED.limit_up,
    "limit_down" = EXCLUDED.limit_down,
    "open_price" = EXCLUDED.open_price,
    "high_price" = EXCLUDED.high_price,
    "low_price" = EXCLUDED.low_price,
    "pre_close" = EXCLUDED.pre_close;
    "bid_price_1" = EXCLUDED.bid_price_1,
    "bid_price_2" = EXCLUDED.bid_price_2,
    "bid_price_3" = EXCLUDED.bid_price_3,
    "bid_price_4" = EXCLUDED.bid_price_4,
    "bid_price_5" = EXCLUDED.bid_price_5,
    "ask_price_1" = EXCLUDED.ask_price_1,
    "ask_price_2" = EXCLUDED.ask_price_2,
    "ask_price_3" = EXCLUDED.ask_price_3,
    "ask_price_4" = EXCLUDED.ask_price_4,
    "ask_price_5" = EXCLUDED.ask_price_5,
    "bid_volume_1" = EXCLUDED.bid_volume_1,
    "bid_volume_2" = EXCLUDED.bid_volume_2,
    "bid_volume_3" = EXCLUDED.bid_volume_3,
    "bid_volume_4" = EXCLUDED.bid_volume_4,
    "bid_volume_5" = EXCLUDED.bid_volume_5,
    "ask_volume_1" = EXCLUDED.ask_volume_1,
    "ask_volume_2" = EXCLUDED.ask_volume_2,
    "ask_volume_3" = EXCLUDED.ask_volume_3,
    "ask_volume_4" = EXCLUDED.ask_volume_4,
    "ask_volume_5" = EXCLUDED.ask_volume_5,
    "localtime" = EXCLUDED.localtime;
"""

SAVE_TICKOVERVIEW_QUERY: str = """
INSERT INTO pg.public.dbtickoverview VALUES (
    $symbol, $exchange, $count, $start, $end
)
ON CONFLICT DO UPDATE SET
    "count" = EXCLUDED.count,
    "start" = EXCLUDED.start,
    "end" = EXCLUDED.end;
"""

LOAD_TICK_QUERY: str = """
SELECT * FROM pg.public.dbtickdata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "datetime" >= $start
    AND "datetime" <= $end
ORDER BY "datetime" ASC;
"""

LOAD_TICKOVERVIEW_QUERY: str = """
SELECT * FROM pg.public.dbtickoverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
"""

COUNT_TICK_QUERY: str = """
SELECT COUNT("last_price") FROM pg.public.dbtickdata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

DELETE_BAR_QUERY: str = """
DELETE FROM pg.public.dbbardata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

DELETE_BAROVERVIEW_QUERY: str = """
DELETE FROM pg.public.dbbaroverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
    AND "interval" = $interval
"""

DELETE_TICK_QUERY: str = """
DELETE FROM pg.public.dbtickdata
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
"""

DELETE_TICKOVERVIEW_QUERY: str = """
DELETE FROM pg.public.dbtickoverview
WHERE "symbol" = $symbol
    AND "exchange" = $exchange
"""

LOAD_ALL_BAROVERVIEW_QUERY: str = """
SELECT * FROM pg.public.dbbaroverview;
"""

LOAD_ALL_TICKOVERVIEW_QUERY: str = """
SELECT * FROM pg.public.dbtickoverview;
"""

class DuckdbDatabase(BaseDatabase):
    """
    基于 DuckDB 引擎 + PostgreSQL 存储的数据库接口。
    DuckDB 用于前端高性能数据处理，PostgreSQL 用于后端持久化存储。
    """

    def __init__(self) -> None:
        """"""
        self.db : duckdb.DuckDBPyConnection = db
        self.cursor: duckdb.DuckDBPyConnection = self.db.cursor()
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
        interval: Interval = bar.interval

        # 转换为 DuckDB 友好的列表格式 (list of Tuples)
        # 顺序必须与 INSERT 语句中的列顺序一致
        data: list[dict] = []

        for bar in bars:
            bar.datetime = convert_tz(bar.datetime)

            d: dict = bar.__dict__
            d["exchange"] = d["exchange"].value
            d["interval"] = d["interval"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        df: pl.DataFrame = pl.from_records(data, schema=[ 
            "symbol",
            "exchange",
            "interval",
            "datetime",
            "volume",
            "turnover",
            "open_interest",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
        ])

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
                "start": data[0]["datetime"],
                "end": data[-1]["datetime"],
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
                "start": min(data[0]["datetime"], row[4]),
                "end": max(data[-1]["datetime"], row[5]),
                "count": count
            }

        self.execute(SAVE_BAROVERVIEW_QUERY, data)

        return True

    def save_tick_data(self, ticks: list[TickData], stream: bool = False) -> bool:
        """保存TICK数据"""
        tick: TickData = ticks[0]
        symbol: str = tick.symbol
        exchange: Exchange = tick.exchange

        data: list[dict] = []

        for tick in ticks:
            tick.datetime = convert_tz(tick.datetime)

            d: dict = tick.__dict__
            d["exchange"] = d["exchange"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        df: pl.DataFrame = pl.from_records(data, schema=[ 
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
        ])

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
                "start": data[0]["datetime"],
                "end": data[-1]["datetime"],
                "count": len(ticks)
            }
        # Existing contract
        else:
            self.execute(COUNT_TICK_QUERY, params)
            count = self.cursor.fetchone()[0]

            data: dict = {
                "symbol": symbol,
                "exchange": exchange.value,
                "start": min(data[0]["datetime"], row[4]),
                "end": max(data[-1]["datetime"], row[5]),
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