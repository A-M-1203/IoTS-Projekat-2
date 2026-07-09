const metrics = {
  received: 0,
  stored: 0,
  errors: 0,
  buffered: 0,
  batchesFlushed: 0,
};

function getMetrics() {
  return { ...metrics };
}

function incrementReceived() {
  metrics.received += 1;
  if (metrics.received % 1000 === 0) {
    console.log(`Received ${metrics.received} messages so far.`);
  }
}

function incrementStored(count) {
  metrics.stored += count;
}

function incrementErrors() {
  metrics.errors += 1;
}

function setBuffered(count) {
  metrics.buffered = count;
}

function incrementBatchesFlushed() {
  metrics.batchesFlushed += 1;
}

module.exports = {
  getMetrics,
  incrementReceived,
  incrementStored,
  incrementErrors,
  setBuffered,
  incrementBatchesFlushed,
};
