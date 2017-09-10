const fs = require('fs');
const MongoClient = require('mongodb').MongoClient;
const url = "mongodb://localhost:27017/parsedTSV";
const async = require('asyncawait/async');
const await = require('asyncawait/await');
const _ = require('lodash');

class Parser {

    writeTSVToDb(name) {

        let currentCollectionName = '';
        let currentCollectionFields = [];
        let skipRecord = false;
        const existingTables = [];
        const arr = this.parseTSV(name);
        const conn = this.getMongoConnection()
        Promise.all([arr, conn]).then(async((result) => {
            const parsedFile = result[0];
            const db = result[1];

            for (let i = 0; i < parsedFile.length; i++) {
                if (parsedFile[i][0] == '%T') {
                    skipRecord = this.checkTableExisting(existingTables, parsedFile[i][1])
                    if (skipRecord) {
                        continue;
                    }
                    currentCollectionName = parsedFile[i][1];
                    currentCollectionFields = parsedFile[i+1];
                    existingTables.push(parsedFile[i][1]);
                    await(this.createCollection(parsedFile[i][1], db))
                }
                if (parsedFile[i][0] == '%R' &&
                    !skipRecord) {
                    const objToCollection =
                        this.getObject(currentCollectionFields, parsedFile[i]);
                    await(this.addRecord(objToCollection, currentCollectionName, db))
                }
            }
            const task = await(this.getCollection(db, 'TASK'));
            console.log(task);
            db.close();
        }));
    }

    getObject(key, value) {
        const obj = _.zipObject(key, value);
        _.unset(obj, '%F');
        return obj
    }

    checkTableExisting(existingTables, newTable) {
        if (existingTables.indexOf(newTable) == -1) {
            return false
        } else {
            return true
        }
    }

    parseTSV(name) {
        return new Promise((resolve, reject) => {
            fs.readFile(name, { encoding : 'utf8' }, (err, data) => {
                if (err) {
                    reject(err);
                    return
                }
                const fileAsArray = [];
                data.split('\r\n').forEach(line => {
                    const lineAsArray = line.split('\t');
                    fileAsArray.push(lineAsArray);
                });
                resolve(fileAsArray);
            });
        })
    }

    getMongoConnection() {
        return new Promise((resolve, reject) => {
            MongoClient.connect(url, (err, db) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(db);
            });
        })
    }

    createCollection(name, db) {
        return new Promise((resolve, reject) => {

            db.createCollection(name, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }
                console.log('Collection created!')
                resolve(res);
            });
        })
    }

    addRecord(obj, collectionName, db) {
        return new Promise((resolve, reject) => {

            db.collection(collectionName).insertOne(obj, (err, res) => {
                if (err) {
                    reject(err);
                    return;
                }
                console.log('1 document inserted');
                resolve(res);
            });
        })
    }

    getCollection(db, collectionName) {
        return new Promise((resolve, reject) => {
            db.collection(collectionName).find().limit(5).toArray((err, res) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(res);
            });
        })
    }
}

const parser = new Parser();
parser.writeTSVToDb('График.xer');