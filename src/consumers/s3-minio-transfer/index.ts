import http from "https";
// import fs from "fs";

const cfsign = require("aws-cloudfront-sign");
const Minio = require("minio");

const minioClient = new Minio.Client({
    endPoint: process.env.MINIO_ENDPOINT,
    port: parseInt(process.env.MINIO_PORT, 10),
    useSSL: true,
    accessKey: process.env.MINIO_ACCESS_KEY,
    secretKey: process.env.MINIO_SECRET_KEY,
});

const CF_PRIVATE_KEY = process.env.CF_PRIVATE_KEY.replace(/\\n/g, "\n");
const CF_PUBLIC_KEY = process.env.CF_PUBLIC_KEY;
const CF_URL = process.env.CF_ENDPOINT;

async function download(url, buffer) {
    return new Promise((resolve, reject) => {
        // const file = fs.createWriteStream(dest, { flags: "wx" });

        const request = http.get(url, response => {
            if (response.statusCode === 200) {
                response.on("data", chunk => {
                    buffer.push(chunk);
                });
                response.on("end", () => {
                    resolve(true);
                });

                response.on("error", () => {
                    reject(false);
                });
            } else {
                // file.close();
                reject(`Server responded with ${response.statusCode}: ${response.statusMessage}`);
            }
        });

        request.on("error", err => {
            // file.close();
            reject(err.message);
        });

        // file.on("finish", () => {
        //     file.close();
        //     resolve(true);
        // });
        // file.on("error", () => {
        //     // file.close();
        //     reject(false);
        // });
    });
}

export async function onMsg(msg: any) {
    /* sample msg
        [{
            "modified": "2022-08-18 10:20:36",
            "size": "1009.1 KiB",
            "path": "redshift_backup/classzoo1/branch_events_2020/part01/partition_date=2020-01-01/event_name=BRANCH_DEEPLINK/0019_part_00.parquet",
            "s3Location": "s3://dn-data-engineering-data"
        }]
    */
    for (let i = 0; i < msg.length; i++) {
        // const tempFilePath = "downloaded";
        // try {
        //     fs.unlinkSync(tempFilePath);
        // } catch (err) {
        //     console.log(err);
        // }

        console.log(msg[i]);
        const { s3Location, path } = msg[i];

        const expireTime = Date.now() + 5 * 60 * 1000;
        const cfSigningParams = {
            keypairId: CF_PUBLIC_KEY,
            privateKeyString: CF_PRIVATE_KEY,
            // Optional - this can be used as an alternative to privateKeyString
            // privateKeyPath: '/path/to/private/key',
            expireTime,     // 5 mins
        };
        // Generating a signed URL
        const signedUrl = cfsign.getSignedUrl(
            `${CF_URL}${path}`,
            cfSigningParams
        );

        const buffer = [];
        await download(signedUrl, buffer);

        const minioBucket = s3Location.replace("s3://", "");
        if (!buffer.length) {
            throw new Error("no file downloaded");
        }
        await minioClient.putObject(minioBucket, path, Buffer.concat(buffer));
    }
}

export const opts = [{
    topic: "s3.minio.transfer",
    fromBeginning: true,
    numberOfConcurrentPartitions: 1,
}];
