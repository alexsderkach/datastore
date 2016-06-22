Context.text("key1")
    .reduce(function (intermediate, line) {
        return line;
    })
    .sorted(function (a, b) {
        return parseFloat(a) - parseFloat(b);
    })
    .filter(function (line) {
        return parseFloat(line) % 2 == 1;
    })
    .map(function (line) {
        return line + "hi";
    })
    .min(function (line) {
        return parseFloat(line);
    })
    .sorted()
    .save("key2");


// + map
// + filter
//
// + reduce
// + min
// + max
// + count
//
// + sorted