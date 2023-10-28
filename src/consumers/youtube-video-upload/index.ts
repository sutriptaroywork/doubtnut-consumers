import fs from "fs";
import { promisify } from "util";
import _ from "lodash";
import { google } from "googleapis";
import { S3 } from "aws-sdk";
import puppeteer from "puppeteer";
import sharp from "sharp";

import { mysql, config } from "../../modules";

const readFileAsync = promisify(fs.readFile);

const OUTPUT_WIDTH = 640;
const PNG_COMPRESSION_OPTIONS = {
    compressionLevel: 9,
    quality: 50,
    effort: 10,
    adaptiveFiltering: true,
};

const s3 = new S3({
    region: "ap-south-1",
    signatureVersion: "v4",
});

const { client_secret, client_id } = config.youtubeApi;
console.log({ client_secret, client_id });
const youtube = google.youtube("v3");
const oAuth2Client = new google.auth.OAuth2(
    client_id, client_secret);


function getChannelTokens(channelId) {
    const sql = "select refresh_token from youtube_channel where id = ?";
    return mysql.con.query(sql, [channelId]).then(x => x[0]);
}

function insertVideoEvent(eventId, channelId, youtubeId, streamKey = null) {
    const sql = "insert ignore into youtube_channel_event_map set event_id = ?, channel_id = ?, youtube_id = ?, stream_key = ?";
    return mysql.con.query(sql, [eventId, channelId, youtubeId, streamKey]).then(x => x[0]);
}


async function getResource(liveclassResourceId) {
    const sql = "SELECT a.id as detail_id, d.class, a.live_at,hour(a.live_at) as hour_class, minute(a.live_at) as minute_class, a.subject as subject_class, a.liveclass_course_id, a.chapter, b.resource_reference, c.name as faculty_name ,c.image_url as faculty_image,d.title from liveclass_course_resources as b left join liveclass_course_details as a on a.id=b.liveclass_course_detail_id left join dashboard_users as c on a.faculty_id=c.id left JOIN liveclass_course as d on a.liveclass_course_id=d.id left join course_details_liveclass_course_mapping as e on a.liveclass_course_id = e.liveclass_course_id  where b.id = ? GROUP by a.id ORDER BY a.live_at  DESC";
    return mysql.con.query(sql, [liveclassResourceId]).then(x => x[0]);
}

function updateEventState(eventId, isProcessed) {
    const sql = "update youtube_event set is_processed = ? where id = ?";
    return mysql.con.query(sql, [isProcessed, eventId]).then(x => x[0]);
}

async function generateThumbnail(liveclassResourceId) {
    const browser = await puppeteer.launch({
        headless: true,
        args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
    const page = await browser.newPage();

    const resourceDetails = await getResource(liveclassResourceId);
    const {
        subject_class: subject,
        chapter: lecture,
        faculty_image: facultyImage,
        resource_reference: questionId,
        faculty_name: facultyName,
        title,
        hour_class: hourClass,
        minute_class: minuteClass,
    } = resourceDetails[0];

    const timeClass1 = hourClass % 12 || 12; // Adjust hours
    const timeClass2 = hourClass < 12 ? "AM" : "PM"; // Set AM/PM
    const classTime = !minuteClass ? `${timeClass1} ${timeClass2}` : `${timeClass1}:${minuteClass} ${timeClass2}`;

    await page.setViewport({
        width: 970,
        height: 465,
        deviceScaleFactor: 1,
    });

    await page.setDefaultNavigationTimeout(300000);
    let template = await readFileAsync("./src/consumers/youtube-video-upload/live01a.html", "utf8");

    template = template.replace("#top#", title);
    template = template.replace("#time#", classTime);
    template = template.replace("#subject#", subject);
    template = template.replace("#chapter#", lecture);
    template = template.replace("#expert#", facultyName);
    template = template.replace("#image#", facultyImage);
    template = template.replace(/#staticCDN#/g, config.cdnUrl);

    await page.setContent(template);
    const image = await page.screenshot({ type: "png" });
    await page.close();
    await browser.close();
    const resizedPngImage = await sharp(image).resize(OUTPUT_WIDTH).png(PNG_COMPRESSION_OPTIONS).toBuffer();

    return resizedPngImage;
}

export async function onMsg(msg: any) {
    for (let m = 0; m < msg.length; m++) {
        try {
            const { eventId, channels, resourceType, title, description, liveAt, resource, oldResourceId } = msg[m].data;

            const thumbnail = await generateThumbnail(oldResourceId.old_resource_id);

            console.log("thumbnail generated");
            let resSplit = resource.split("/");
            resSplit = resSplit.slice(3, resSplit.length - 1);
            let origResource = resSplit.join("/") + ".mp4";
            origResource = origResource.replace("DN/LC/SCHEDULED-VOD", "SCHEDULED-VOD");
            const backupResource = resSplit.join("/") + "/h264_480p.mp4";

            let s3Bucket = "dn-original-studio-videos";
            let s3Resource = origResource;
            try {
                const data = await s3.headObject({
                    Bucket: s3Bucket,
                    Key: s3Resource,
                }).promise();
                if (_.get(data, "StorageClass", null) === "DEEP_ARCHIVE") {
                    const e = new Error("Object is Archived");
                    e.name = "ArchivedObject";
                    throw e;
                }
            } catch (err) {
                if (err.name === "NotFound" || err.name === "ArchivedObject") {
                    // Handle no object on cloud here...
                    s3Bucket = "dn-streaming-backup";
                    s3Resource = backupResource;
                    try {
                        await s3.headObject({
                            Bucket: s3Bucket,
                            Key: s3Resource,
                        }).promise();
                    } catch (err2) {
                        console.log(err2);
                        continue;
                    }
                } else {
                    // Handle other errors here....
                    console.log(err);
                    continue;
                }
            }
            console.log(s3Bucket, s3Resource);
            for (let i = 0; i < channels.length; i++) {
                const channelId = channels[i];
                console.log("for channel", channelId);

                const channelToken: any = await getChannelTokens(channelId);
                if (!channelToken.length) {
                    continue;
                }
                console.log({ ...channelToken[0] });

                oAuth2Client.setCredentials({ ...channelToken[0] });
                await oAuth2Client.refreshAccessToken();
                google.options({ auth: oAuth2Client });

                if (parseInt(resourceType, 10) === 1) {
                    const s3data = {
                        Bucket: s3Bucket,
                        Key: s3Resource,
                    };
                    const fileStream = s3.getObject(s3data).createReadStream();

                    const params = {
                        part: ["id", "snippet", "status"],
                        requestBody: {
                            snippet: {
                                title,
                                description,
                            },
                            status: {
                                privacyStatus: "private",
                                publishAt: liveAt,
                                embeddable: false,
                                selfDeclaredMadeForKids: false,
                            },
                        },
                        media: {
                            mimeType: "video/mp4",
                            body: fileStream,
                        },
                    };

                    const res: any = await youtube.videos.insert(params);
                    console.log(res);
                    try {
                        const res2: any = await youtube.thumbnails.set({
                            videoId: res.data.id,
                            media: {
                                mimeType: "image/png",

                                body: thumbnail,
                            },
                        });
                        console.log(res2);
                    } catch (err) {
                        console.log("err in thumbnail upload");
                        console.log(err);
                    }
                    await insertVideoEvent(eventId, channelId, res.data.id);
                }
                // else if (resourceType == 4) {
                //     const params = {
                //         part: ["id", "snippet", "status"],
                //         resource: {
                //             snippet: {
                //                 title,
                //                 description,
                //             },
                //             status: {
                //                 privacyStatus: "private",
                //                 publishAt: liveAt,
                //             },
                //         },
                //     };

                //     const res: any = await youtube.liveStreams.insert(params);

                //     await insertVideoEvent(eventId, channelId, res.id, res.key);
                // }
            }

            await updateEventState(eventId, 1);
        } catch (err) {
            console.log(err);
        } finally {
            console.log(`the script successfully ran at ${new Date()}`);
        }
    }

}

export const opts = [{
    topic: "api-server.youtube-video.upload",
    fromBeginning: false,
}];
