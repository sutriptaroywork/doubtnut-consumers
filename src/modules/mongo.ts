const MongoClient = require("mongodb").MongoClient;
const { config } = require("./config");

const db = {
    event: {
        database: "",
    },
    newton: {
        database: "",
    },
    studygroup: {
        database: "",
    },
    teslaFeed: {
        database: "",
    },
};
let client;

async function connectMongo(cluster, database) {
    console.log(`${cluster} mongo, db : ${database} - ${config.mongo[cluster].database_url}`);
    client = await MongoClient.connect(config.mongo[cluster].database_url, {
        keepAlive: true,
        useNewUrlParser: true,
        useUnifiedTopology: true,
        connectTimeoutMS: 120000,
        maxIdleTimeMS: 12000,
    });
    db[cluster][database] = client.db(database);
    return db[cluster][database];
}

/**
 * @returns {Promise<mongo.MongoClient>}
 */
export async function getMongoDB(cluster, database) {
    if (db && db[cluster] && db[cluster][database]) {
        console.log("already connected");
        return db[cluster][database];
    }
    return connectMongo(cluster, database);
}
