import dozer_log

async def main():
    reader = await dozer_log.LogReader.new('../.dozer/pipeline', 'trips')
    for _ in range(10):
        data = await reader.next_op()
        print(data)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
