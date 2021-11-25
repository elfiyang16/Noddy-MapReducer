### Noddy MapReducer

MapReduce is an amazing framework in Handoop system. If you also use AWS for your work, then try EMR to start with. But I wonder if it's possible to have a skeleton version of MapReduce in vanilla NodeJS, hence the project.

Obviously there're a lot of details got left out, but it does the basics (or does it lol).

This MapReduce requires you to define a Mapper and Reducer function (there's example in the `application.js`), as well as a data input (`data.js`).

In the example, it does the classic "word count". The result is a Map object like (I'm using lorem lpsum, and didn't do other pre like removig stop word):

```
Map(74) {
  'eos' => 1,
  'error' => 3,
  'est' => 3,
  'et' => 4,
  'omnis' => 3,
  'perspiciatis' => 3,
  'praesentium' => 1,
  'provident,\nsimilique' => 1,
  'quaerat' => 1,
  'quas' => 1,
  'qui\nblanditiis' => 1,
  'qui' => 1,
  'quidem' => 1,
  'quos' => 1,
  'rerum' => 1,
  'sint' => 1,
  'sit' => 3,
}
```

To run the program:

```
node application.js
```
