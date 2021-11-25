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
function* convert_hashmap_to_array(obj) {
  //   for (let key in obj) yield new Map().set(key, obj[key]);
  for (let key in obj) yield [key, obj[key]];
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
  reducer_tasks.sort();
  return reducer_tasks;
};
const replacer = function (key, value) {
  if (value instanceof Map) {
    return {
      dataType: "Map",
      value: Array.from(value.entries()), // or with spread: value: [...value]
    };
  } else {
    return value;
  }
};
const reviver = function (key, value) {
  if (typeof value === "object" && value !== null) {
    if (value.dataType === "Map") {
      return new Map(value.value);
    }
  }
  return value;
};

const task_mgr = function (input, map, reduce, callback) {
  const tasks = split_chunk(input);

  if (cluster.isMaster) {
    for (let i = 0; i < cores; i++) {
      var worker = cluster.fork();
      var map_count = 0;
      var reduce_count = 0;
      var post_mapper = [];
      var post_reducer = [];
      var reducer_task_size;
      const task = tasks[worker.id - 1];

      worker.send({ map_task: task });

      worker.on("message", (msg) => {
        if (msg.signal === "MapCompleted") {
          post_mapper = post_mapper.concat(msg.mapped);
          map_count++;

          if (map_count === cores) {
            console.log(post_mapper);
            //  e.g. { 'a': [ 1 ], 'b': [ 1 ], "c": [ 1, 1, 1 ] }
            const key_values = shuffle(post_mapper);
            const reducer_tasks = partition(key_values);
            reducer_task_size = reducer_tasks.length;
            for (const task of reducer_tasks) {
              worker.send({ reduce_task: task });
            }
          }
        }
        if (msg.signal === "ReduceCompleted") {
          post_reducer = post_reducer.concat(Object.entries(msg.reduced));
          reduce_count++;

          if (reduce_count === reducer_task_size) {
            callback(new Map(post_reducer));
          }
        }
      });

      worker.on("error", (error) => {
        console.log(error);
      });

      worker.on("exit", (code, signal) => {
        if (code !== 0) {
          console.log(`worker exited with error code: ${code}`);
        }
      });

      cluster.on("exit", (worker, code) => {
        if (reduce_count === reducer_task_size) {
          // kill remaining workers
          for (const id in cluster.workers) {
            const process_id = cluster.workers[id].process.pid;
            // process.kill(process_id)
            cluster.workers[id].kill();
          }
          process.exit(0);
        }
      });
    }
  } else if (cluster.isWorker) {
    process.on("message", (msg) => {
      if (msg["map_task"]) {
        const mapped = map(msg["map_task"]);
        process.send({
          from: cluster.worker.id,
          signal: "MapCompleted",
          mapped,
        });
      }
      if (msg["reduce_task"]) {
        let reduced = {};
        msg["reduce_task"].map((el) => {
          reduced[el[0]] = reduce(el[0], el[1]);
        });

        process.send({
          from: cluster.worker.id,
          signal: "ReduceCompleted",
          reduced,
        });
        cluster.worker.kill();
      }
    });
  }
};

module.exports = function (c) {
  cores = c || default_cores;
  return task_mgr;
};
