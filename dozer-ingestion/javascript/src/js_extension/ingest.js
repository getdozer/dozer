(async () => {
    const url = 'https://api.github.com/repos/getdozer/dozer/commits';
    const response = await fetch(url);

    const commits = await response.json();

    const snapshot_msg = { typ: "SnapshottingDone", old_val: null, new_val: null };
    await Deno[Deno.internal].core.ops.ingest(snapshot_msg);

    for (const commit of commits) {
        const msg = {
            typ: "Insert",
            old_val: null,
            new_val: { commit: commit.sha },
        };
        await Deno[Deno.internal].core.ops.ingest(msg);
    }
})();
