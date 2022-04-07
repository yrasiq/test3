import uvicorn, aioredis, asyncpg, random, configparser
from fastapi import FastAPI

cfg = configparser.ConfigParser()
cfg.read(".env")
cfg_fastapi = cfg["FastAPI"]
REDIS_HOST = cfg_fastapi.get("REDIS_HOST", "redis://localhost")
UVICORN_HOST = cfg_fastapi.get("UVICORN_HOST", "0.0.0.0")
UVICORN_PORT = int(cfg_fastapi.get("UVICORN_PORT", "8000"))
POSTGRES_USER = cfg_fastapi.get("POSTGRES_USER", "postgres")
POSTGRES_DATABASE = cfg_fastapi.get("POSTGRES_DATABASE", "postgres")
POSTGRES_PASSWORD = cfg_fastapi.get("POSTGRES_PASSWORD", "")
POSTGRES_HOST = cfg_fastapi.get("POSTGRES_HOST", "127.0.0.1")


app = FastAPI()
app.state.devices_example = ["emeter", "zigbee", "lora", "gsm"]


@app.on_event("startup")
async def redis_init():
    redis = await aioredis.from_url(REDIS_HOST, decode_responses=True)
    await redis.set("is_anagram_counter", 0)
    app.state.redis = redis

@app.on_event("shutdown")
async def redis_close():
    await app.state.redis.close()

@app.on_event("startup")
async def pg_init():
    pg = await asyncpg.connect(
        user=POSTGRES_USER,
        database=POSTGRES_DATABASE,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST
    )
    app.state.pg = pg

@app.on_event("shutdown")
async def pg_close():
    app.state.pg.close()


@app.get("/is_anagram/")
async def is_anagram(str1: str, str2: str):
    str1 = set(str1)
    str2 = set(str2)
    is_anagram = True if str1 == str2 else False
    counter = int(await app.state.redis.get("is_anagram_counter"))
    if is_anagram:
        counter += 1
        await app.state.redis.set("is_anagram_counter", counter)
    return {}

@app.post("/devices/", status_code=201)
async def post_devices():
    rand = [
        (
            app.state.devices_example[random.randint(0, 3)], 
            bytearray([random.randint(0, 255) for i in range(6)]).hex()
        )
        for i in range(10)
    ]
    values_dev = ",\n".join([f"('{device[0]}', '{device[1]}')" for device in rand])
    async with app.state.pg.transaction():
        ids = await app.state.pg.fetch('''
            INSERT INTO devices (dev_type, dev_id)
            VALUES
            {}
            RETURNING id;
        '''.format(values_dev)
        )
        rand_ids = random.sample([i.get("id") for i in ids], k=5)
        values_endps = ",\n".join([f"({i})" for i in rand_ids])
        await app.state.pg.execute('''
            INSERT INTO endpoints (device_id)
            VALUES
            {};
        '''.format(values_endps)
        )
    return {}

@app.get("/devices/")
async def get_devices():
    dev = await app.state.pg.fetch('''
        SELECT dev_type, COUNT(dev_type) FROM devices dv
        LEFT JOIN endpoints ep
        ON dv.id = ep.device_id
        WHERE ep.device_id IS NULL
        GROUP BY dev_type;
    ''')
    return dev


if __name__ == "__main__":
    uvicorn.run("main:app", port=UVICORN_PORT, host=UVICORN_HOST)

