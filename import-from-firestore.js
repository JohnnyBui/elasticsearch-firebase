const admin = require('firebase-admin');
const elasticsearch = require('elasticsearch');

// Config before running
const CONFIG = {
  limit: 1000, // reduce if got Deadline exceeded error
  index: 'stock',
  type: 'doc',
  dev: {
    serviceAccountJson: './devServiceAccount.json',
    databaseUrl: 'https://<projectId>.firebaseio.com',
    elasticUrl: '<elasticSearchUrl>'
  },
  prod: {
    serviceAccountJson: './prodServiceAccount.json',
    databaseUrl: 'https://<projectId>.firebaseio.com',
    elasticUrl: '<elasticSearchUrl>'
  }
}

const serviceAccount = require(CONFIG.dev.serviceAccountJson);
const databaseURL = CONFIG.dev.databaseUrl;
const elasticUrl = CONFIG.dev.elasticUrl;
let currentCursor = 0;
let startTime;

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: databaseURL
});
const db = admin.firestore();
const esClient = new elasticsearch.Client({ host: elasticUrl });

function getNext(startAfterDocument) {
  db.collection(CONFIG.index).startAfter(startAfterDocument).limit(CONFIG.limit).get().then(collection => processBatch(collection));
}

function processBatch(collection) {
  if (!collection.size) {
    console.log('Job finished after', (Date.now() - startTime) / 1000 / 60, 'minutes');
    return;
  }

  const allDocs = collection.docs;
  let requestData = [];
  for (const doc of allDocs) {
    requestData.push({ index: { _index: CONFIG.index, _type: CONFIG.type, _id: doc.id } });
    requestData.push(doc.data());
  }

  esClient.bulk({ body: requestData }, (err, result) => {
    if (err) {
      return console.log('Error', err.message);
    }

    currentCursor += collection.size;
    console.log(`Current Progress: ${currentCursor}.`, 'Last Document ID in batch:', collection.docs[collection.docs.length - 1].id);
    getNext(collection.docs[collection.docs.length - 1]);
  });
}

esClient.deleteByQuery({
  index: CONFIG.index,
  body: {
    query: {
      match_all: {}
    }
  }
}, (err, result) => {
  if (err) {
    return console.log('Error', err.message);
  }
  console.log('Cleared data on Elasticsearch.');
  console.log('Started syncing...');
  startTime = Date.now();
  return db.collection(CONFIG.index).limit(CONFIG.limit).get().then(collection => processBatch(collection));
});
