import mysql2 from "mysql2";
import { config } from "./config";

const con = mysql2.createPool({
    host: config.mysql.host.read,
    port: 3306,
    database: config.mysql.database,
    user: config.mysql.user,
    password: config.mysql.password,
    connectionLimit: 200,
    enableKeepAlive: true,
    multipleStatements: true,
    queueLimit: 500,
    connectTimeout: parseInt(config.mysql.connectionTimeout.toString(), 10),
});

const writeCon = mysql2.createPool({
    host: config.mysql.host.write,
    port: 3306,
    database: config.mysql.database,
    user: config.mysql.user,
    password: config.mysql.password,
    connectionLimit: 200,
    enableKeepAlive: true,
    multipleStatements: true,
    queueLimit: 500,
});

async function singleQueryTransaction(sql, args) {
    const conCopy = con.promise();
    const conn = await conCopy.getConnection();
    conn.beginTransaction();
    try {
        const [rows] = await conn.query(sql, args);
        await conn.commit();
        return [rows];
    } catch (error) {
        await conn.rollback();
    }
    finally {
        conn.release();
    }
}


export const mysql = {
    con: con.promise(),
    writeCon: writeCon.promise(),
    singleQueryTransaction,
};
