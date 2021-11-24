const mapreduce = require("./index")();
const input = require("./data");

const map = function (input) {
  const acc = new Map();
  input.forEach(function (value) {
    if (acc.has(value)) {
      acc.set(value, acc.get(value) + 1);
    } else {
      acc.set(value, 1);
    }
  });

  return [...acc];
};

const reduce = function (key, values) {
  let sum = 0;
  values.forEach(function (e) {
    sum += e;
  });
  return sum;
};

mapreduce(input, map, reduce, function (result) {
  console.log(result);
});
