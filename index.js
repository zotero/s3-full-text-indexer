/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2019 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const mysql2 = require('mysql2');
const mysql2Promise = require('mysql2/promise');
const sqlite = require('sqlite');
const through2Concurrent = require('through2-concurrent');
const config = require('config');
const elasticsearch = require('elasticsearch');
const AWS = require('aws-sdk');
const zlib = require('zlib');
const fs = require('fs');

const s3 = new AWS.S3(config.get('s3'));

let dbPath = './db/';

let s3MissingKeysLog = 's3_missing.log';

let sqliteShard = null;

let numProcessedPrev = 0;
let numProcessed = 0;
let numConflicts = 0;

let currentShardId = null;

let lastShardDate = null;
let activeDates = {};

let bulkSize = 500;
let bulk = [];

const es = new elasticsearch.Client({
	host: config.get('es.host'),
	log: 'error'
});

async function getShardDate(shardID) {
	let row = await sqliteShard.get(
		'SELECT shardDate FROM shard WHERE shardID = ?',
		[shardID]
	);
	if (!row) return null;
	return row.shardDate;
}

async function setShardDate(shardID, shardDate) {
	await sqliteShard.run(
		'INSERT OR REPLACE INTO shard (shardID, shardDate) VALUES (?,?)',
		[shardID, shardDate]
	);
}

function removeActiveDate(id) {
	let date = activeDates[id];
	delete activeDates[id];
	let earliestDate = Object.values(activeDates).sort()[0];
	if (!earliestDate || date < earliestDate) {
		lastShardDate = date;
	}
}

async function delay(ms) {
	return new Promise(function (resolve) {
		setTimeout(resolve, ms);
	})
}

async function getItem(id) {
	let data;
	try {
		data = await s3.getObject({Key: id}).promise();
	}
	catch (err) {
		console.log('S3 error for key ' + id + ': ' + err.code);
		if (err.code === 'NoSuchKey') {
			fs.appendFileSync(s3MissingKeysLog, id + '\n');
			return null;
		}
		throw err;
	}
	
	let json = data.Body;
	
	if (data.ContentType === 'application/gzip') {
		json = zlib.unzipSync(json);
	}
	
	json = JSON.parse(json.toString());
	
	return json;
}

function addToBulk(item) {
	bulk.push({
		index: {
			_index: config.get('es.index'),
			_type: config.get('es.type'),
			_id: item.libraryID + '/' + item.key,
			_version: item.version,
			_version_type: 'external_gt',
			_routing: item.libraryID,
		}
	});
	
	bulk.push({
		libraryID: item.libraryID,
		content: item.content
	});
}

async function triggerBulkIndexing(force) {
	if (!bulk.length) return null;
	
	if (!force && bulk.length / 2 < bulkSize) return null;
	
	let result = await es.bulk({body: bulk});
	
	for (let item of result.items) {
		if (item.index.error) {
			if (item.index.error.type === 'version_conflict_engine_exception') {
				numConflicts++;
			}
			else if (item.index.error.type === 'es_rejected_execution_exception') {
				console.log(item.index.error);
				await delay(1000);
				console.log('Retrying..');
				return await triggerBulkIndexing(force);
			}
			else {
				throw new Error(JSON.stringify(item.index.error));
			}
		}
		
		removeActiveDate(item.index._id);
		numProcessed++;
	}
	
	bulk = [];
}

function streamShard(mysqlShard, shardDate) {
	return new Promise(function (resolve, reject) {
		let sql = `
				SELECT I.libraryID, I.key, IFT.timestamp
				FROM itemFulltext IFT
				JOIN items I USING (itemID)
				${shardDate ? (`WHERE IFT.timestamp >= '${shardDate}'`) : ''}
				ORDER BY IFT.timestamp
			`;
		let params = [shardDate];
		
		mysqlShard.query(sql, params)
			.stream({highWaterMark: 1024})
			.pipe(through2Concurrent(
				{
					objectMode: true,
					maxConcurrency: config.get('concurrency')
				},
				async function (row, enc, next) {
					try {
						let id = row.libraryID + '/' + row.key;
						activeDates[id] = row.timestamp;
						let item = await getItem(id);
						if(item) {
							this.push(item);
						}
					}
					catch (err) {
						return reject(err);
					}
					next();
				}
			))
			.pipe(through2Concurrent(
				{
					objectMode: true,
					maxConcurrency: 1
				},
				async function (item, enc, next) {
					try {
						addToBulk(item);
						await triggerBulkIndexing();
					}
					catch (err) {
						return reject(err);
					}
					next();
				}
			))
			.on('data', function () {
			})
			.on('end', async function () {
				try {
					await triggerBulkIndexing(true);
				}
				catch (err) {
					return reject(err);
				}
				resolve();
			});
	});
}

async function createReaderConnection(connectionInfo) {
	return new Promise(function (resolve, reject) {
		let connection = mysql2.createConnection(connectionInfo);
		connection.connect(function (err) {
			if (err) return reject(err);
			connection.query("SHOW GLOBAL VARIABLES LIKE 'innodb_read_only'",
				function (err, result) {
					if (err) return reject(err);
					if (result[0] && result[0].Value === 'ON') return resolve(connection);
					connection.close();
					return resolve();
				}
			);
		});
	});
}

async function getShardReaderConnection(mysqlMaster, shardHostID, shardDb) {
	let [replicas] = await mysqlMaster.execute(
		"SELECT * FROM shardHostReplicas WHERE shardHostID = ? AND state = 'up'",
		[shardHostID]
	);
	
	let i = replicas.length;
	while (--i) {
		let j = Math.floor(Math.random() * (i + 1));
		let tmp = replicas[i];
		replicas[i] = replicas[j];
		replicas[j] = tmp;
	}
	
	for (let i = 0; i < replicas.length; i++) {
		try {
			let replica = replicas[i];
			let connection = await createReaderConnection({
				host: replica.address,
				user: config.get('mysql.user'),
				password: config.get('mysql.password'),
				port: replica.port,
				database: shardDb,
				dateStrings: true
			});
			if (connection) return connection;
		}
		catch (err) {
			console.log(err);
		}
	}
}

async function processShards() {
	let mysqlMaster = await mysql2Promise.createConnection(config.get('mysql'));
	let [shardRows] = await mysqlMaster.execute(
		"SELECT * FROM shards WHERE state = 'up' ORDER BY shardID"
	);
	
	for (let shardRow of shardRows) {
		currentShardId = shardRow.shardID;
		
		let mysqlShard = await getShardReaderConnection(
			mysqlMaster,
			shardRow.shardHostID,
			shardRow.db
		);
		
		if (!mysqlShard) throw new Error(
			'Failed to get a reader connection for shard ' + shardRow.shardID
		);
		
		let shardDate = await getShardDate(shardRow.shardID);
		console.log(`Streaming shard ${shardRow.shardID} from ${shardDate}`);
		await streamShard(mysqlShard, shardDate);
		
		mysqlShard.close();
		
		if (lastShardDate) {
			await setShardDate(currentShardId, lastShardDate);
		}
	}
	
	mysqlMaster.close();
}

async function main() {
	console.time('total time');
	
	sqliteShard = await sqlite.open(dbPath + 'shard.sqlite', {Promise});
	await sqliteShard.run("CREATE TABLE IF NOT EXISTS shard (shardID INTEGER PRIMARY KEY, shardDate TEXT)");
	
	console.log('Processing shards');
	await processShards();
	
	await sqliteShard.close();
	
	console.timeEnd('total time');
	
	clearInterval(printInterval);
	clearInterval(saveInterval);
	
	console.log('finished');
}

let printInterval = setInterval(function () {
	console.log(JSON.stringify({
		currentShardId,
		processedPerSecond: Math.floor((numProcessed - numProcessedPrev) / 10),
		numProcessed,
		numConflicts,
		lastShardDate
	}));
	numProcessedPrev = numProcessed;
}, 1000 * 10);

let saveInterval = setInterval(function () {
	if (lastShardDate) {
		setShardDate(currentShardId, lastShardDate);
	}
}, 1000 * 10);

process.on('unhandledRejection', function (err) {
	throw err;
});

main();
