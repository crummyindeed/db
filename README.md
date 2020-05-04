# crummydb

## what
- A level-style embedded databsese (key-value store) for NodeJS
- Written fully in NodeJS with minimal dependancies

## why
- Available best in class embedded stores relied on compiled binaries which make coss-platform deployment difficult
- Available "pure node" stores were limited to a size that could be held completely in memory

## how

```javascript
const DB = require('crummydb');

//Initialize
var directory = 'testDir'; //data stored here
db = new Naive(directory);
await db.init();

//Upsert a Value
db.put('myKey','My Cool Value');

//Retrieve a Value
await db.get('myKey');

//Remove a Value
db.delete('myKey');

```
