const { init, read } = require('./');
const util = require('util');
const readPromise = util.promisify(read);


(async () => {
  // const reader_ref = init("trips", ".dozer/pipeline/logs/trips");
  const reader_ref = init("trips");

  let count = 0;

  while (count < 10) {
    try {
      let data = await readPromise(reader_ref);
      console.log(data);
    } catch (e) {
      console.log(e);
    }

    count++;
  }


})();