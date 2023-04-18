const dozer = require('..');

async function main() {
    runtime = dozer.Runtime();
    reader = await runtime.create_reader('../.dozer/pipeline', 'trips');

    for (let i = 0; i < 10; ++i) {
        let op = await reader.next_op();
        console.log(op);
    }

    // HACK: Node.js process won't exit. We don't know why yet.
    process.exit(0);
}

main();
