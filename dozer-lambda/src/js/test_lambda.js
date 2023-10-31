function lambda(op) {
    console.log(op);
}

(async () => {
    try {
        await Deno[Deno.internal].core.ops.register_lambda('mock', lambda, lambda);
    } catch (e) {
        console.error(`failed to register lambda: ${e}`);
    }
})();
