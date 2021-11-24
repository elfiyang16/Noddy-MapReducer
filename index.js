const cluster = require("cluster");
const default_cores = require("os").cpus().length;

const split_chunk = function (input) {
  input = input.split(" ");
  const chunk_size = Math.ceil(input.length / cores);
  const input_list = [];
  for (let i = 0; i < input.length; i += chunk_size) {
    const upper_bound = Math.min(i + chunk_size, input.length);
    const batch = input.slice(i, upper_bound);
    input_list.push(batch);
  }
  return input_list;
};

const shuffle = function (post_mapper) {
  post_mapper.sort();

  const key_values = post_mapper.reduce(function (acc, cur) {
    let key = acc[cur[0]];
    if (!key || typeof key != "object") key = [];

    key.push(cur[1]);
    acc[cur[0]] = key;
    return acc;
  }, {});
  return key_values;
};

const mapreduce_cluster = function (input, map, reduce, callback) {
  const tasks = split_chunk(input);

  if (cluster.isMaster) {
    for (let i = 0; i < cores; i++) {
      var worker = cluster.fork();
      var finished = 0;
      var post_mapper = [];

      worker.on("message", (msg) => {
        if (msg.signal === "Completed") {
          post_mapper = post_mapper.concat(msg.intermediate);
        }
      });

      worker.on("error", (error) => {
        console.log(error);
      });

      worker.on("exit", () => {
        finished++;
        if (finished == cores) {
          //  e.g. { 'a': [ 1 ], 'b': [ 1 ], "c": [ 1, 1, 1 ] }
          const key_values = shuffle(post_mapper);
          const key_len = Object.keys(key_values).length;
          const reducer_size = Math.ceil(key_len / cores);

          for (const k in key_values) {
            key_values[k] = reduce(k, key_values[k]);
          }

          callback(key_values);
        }
      });
    }
  } else {
    let task = tasks[cluster.worker.id - 1];

    let intermediate = map(task);

    process.send({
      from: cluster.worker.id,
      signal: "Completed",
      intermediate,
    });

    cluster.worker.kill();
  }
};

module.exports = function (c) {
  cores = c || default_cores;
  return mapreduce_cluster;
};
