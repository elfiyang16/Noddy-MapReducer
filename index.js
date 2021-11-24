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

// To avoid intermediate big array
function* convert_hashmap_to_array(obj) {
  for (let key in obj) yield new Map().set(key, obj[key]);
}

const shuffle = function (post_mapper) {
  post_mapper.sort();

  const key_values = post_mapper.reduce(function (acc, cur) {
    let key = acc[cur[0]];
    if (!key || typeof key != "object") key = [];

    key.push(cur[1]);
    acc[cur[0]] = key;
    return acc;
  }, {});
  const key_values_array = Array.from(convert_hashmap_to_array(key_values));
  return key_values_array;
};

const partition = function (key_values_array) {
  const key_len = key_values_array.length;
  const reducer_size = Math.ceil(key_len / cores);
  const reducer_tasks = [];
  for (let i = 0; i < key_len; i += reducer_size) {
    const upper_bound = Math.min(i + reducer_size, key_len);
    const batch = key_values_array.slice(i, upper_bound);
    reducer_tasks.push(batch);
  }
  return { reducer_size, reducer_tasks };
};

const mapreduce_cluster = function (input, map, reduce, callback) {
  const tasks = split_chunk(input);

  if (cluster.isMaster) {
    for (let i = 0; i < cores; i++) {
      var worker = cluster.fork();
      var map_count = 0;
      var reduce_count = 0;
      var post_mapper = [];
      const task = tasks[worker.id - 1];
      worker.send({ map_task: task });

      worker.on("message", (msg) => {
        if (msg.signal === "MapCompleted") {
          post_mapper = post_mapper.concat(msg.intermediate);
          map_count++;
        }
      });

      worker.on("error", (error) => {
        console.log(error);
      });

      worker.on("exit", () => {
        if (map_count == cores) {
          //  e.g. { 'a': [ 1 ], 'b': [ 1 ], "c": [ 1, 1, 1 ] }
          const key_values = shuffle(post_mapper);
          const { reducer_size, reducer_tasks } = partition(key_values);
          let final = {};
          for (const reducer_task of reducer_tasks) {
            reducer_task.forEach((pair) => {
              const twisted = Array.from(pair)[0];
              final[twisted[0]] = reduce(twisted[0], twisted[1]);
            });
          }

          callback(final);
        }
      });
    }
  } else {
    let task = tasks[cluster.worker.id - 1];
    let intermediate = map(task);
    process.send({
      from: cluster.worker.id,
      signal: "MapCompleted",
      intermediate,
    });

    cluster.worker.kill();
  }
};

module.exports = function (c) {
  cores = c || default_cores;
  return mapreduce_cluster;
};
