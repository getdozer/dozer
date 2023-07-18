import pydozer_log

async def main():
    reader = await pydozer_log.LogReader.new('http://127.0.0.1:50053', 'trips_data')
    for _ in range(10):
        data = await reader.next_op()
        print(data)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
