/* eslint-disable eqeqeq */
import https from "https";
import fs from "fs";
import { exec } from "child_process";
import readline from "readline";
import _ from "lodash";
import { google } from "googleapis";
import { config, mysql } from "../../modules";
import { data } from "./data";

const { client_secret, client_id } = config.youtubeApi;
const youtube = google.youtube("v3");
const oAuth2Client = new google.auth.OAuth2(client_id, client_secret);

async function isYtUploaded(answerId) {
    const sql = `select * from answers where answer_id='${answerId}' and youtube_id is not null`;
    return mysql.con.query(sql).then(x => x[0]);
}

async function getChannelTokens(channelId) {
    const sql = "select refresh_token from youtube_channel where channel_id = ?";
    return mysql.con.query(sql, [channelId]).then(x => x[0]);
}

async function setYoutubeUploads(obj) {
    const sql = "INSERT INTO youtube_uploads set ?";
    return mysql.writeCon.query(sql, [obj]);
}

async function updateAnswerYoutubeIdByAnswerId(youtubeId, answerId) {
    const sql = `update answers set youtube_id='${youtubeId}' where answer_id='${answerId}'`;
    return mysql.writeCon.query(sql);
}

function addslashes(str: string) {
    return str.replace(/\\/g, "\\\\").
    replace(/\u0008/g, "\\b").
    replace(/\t/g, "\\t").
    replace(/\n/g, "\\n").
    replace(/\f/g, "\\f").
    replace(/\r/g, "\\r").
    replace(/'/g, "\\'").
    replace(/"/g, '\\"');
}


function cleanText(text: string) {
    text = text.replace(/<br>/g, " ");
    text = text.replace(/`\(##/g, "###");
    text = text.replace(/##\)`/g, "###");
    text = text.replace(/<img[^>]+>/g, "");
    text = text.replace(/;/g, ",");
    text = text.replace(/->/g, "rarr");
    text = text.replace(/=>/g, "implies");
    text = text.replace(/<-/g, "larr");
    text = text.replace(/>=/g, "ge");
    text = text.replace(/<=/g, "le");
    text = text.replace(/>/g, "gt");
    text = text.replace(/</g, "lt");
    text = text.replace(/  /g, "");
    text = text.replace(/"/g, "");
    text = addslashes(text);
    text = text.replace(/ dot /g, "");
    text = text.replace(/[\n\r]/g, " ");
    text = text.replace(/\s+/g, " ");
    text = text.replace(/[ ]{2,}|[\t]/g, " ");
    text = text.replace(/!s+!/g, "");
    text = text.replace(/\xc2\xa0/g, " ");
    text = text.replace(/xc2\xa0/g, " ");
    text = text.replace(/[[:^print:]]/g, "");
    text = text.replace(/`/g, "");
    return text;
}

function getYoutubeMeta(answerDetails, hindiBooks) {
    const title = data.title
        .replace("##ocr_text##", answerDetails.ocr_text)
        .replace("##class##", `CLASS ${answerDetails.class}`)
        .replace("##chapter##", answerDetails.chapter)
        .replace("##subject##", answerDetails.subject)
        .replace("##book##", answerDetails.package);
    // eslint-disable-next-line radix
    const keywords = data.keywords[parseInt(answerDetails.class)].map(x=>x).join("\n");
    const books = hindiBooks.map(x=>x.books).join(",");
    const description = data.description
        .replace("##ocr_text##", answerDetails.ocr_text)
        .replace("##class##", `CLASS ${answerDetails.class}`)
        .replace("##chapter##", answerDetails.chapter)
        .replace("##subject##", answerDetails.subject)
        .replace("##book##", answerDetails.package)
        .replace("##board##", answerDetails.target_group)
        .replace("##keywords_by_class##", keywords)
        .replace("##books_name##", books);
    return {title: title.length > 96 ? `${title.substring(0, 96)}...` : title, description : description.length > 4996 ? `${description.substring(0, 4996)}...` : description};
}

async function download(url, fpath) {
    if (fs.existsSync(fpath)) {
        return;
    }
    return new Promise<void>(resolve => {
        https.get(url, res => {
            const filePath = fs.createWriteStream(fpath);
            res.pipe(filePath);
            filePath.on("finish", () => {
                filePath.close();
                console.log(`Downloaded File ${fpath}`);
                resolve();
            });
        });
    });
}

async function doCommand(command) {
    console.log("command", command);
    return new Promise((resolve, reject) => {
        exec(command, (err, stdout, stderr) => {
            if (err) {
                console.log(err);
                reject();
            }
            console.log(stdout);
            console.error(stderr);
            resolve(stdout);
        });
    });
}

async function deleteFile(filePath) {
    if (!fs.existsSync(filePath)) {
        return;
    }
    return new Promise<void>((resolve, reject) => {
        fs.unlink(filePath, err => {
            if (err) {
                console.log(err);
                reject();
            } else {
                console.log(`Deleted file: ${filePath}`);
                resolve();
            }
        });
    });
}

async function deleteFileArr(filePathArr: any) {
    let promises = [];
    for (let i = 0; i < filePathArr.length; i++) {
        promises.push(deleteFile(filePathArr[i]));
        if ( (i % 3 == 0 && i != 0) || i == filePathArr.length - 1) {
            await Promise.all(promises);
            promises = [];
        }
    }
}

function resCheck(resolution) {
    const resArray = resolution.split("x");
    if (resArray[0] / 1280 === 1 || resArray[1] / 720 === 1) {
        return true;
    }
    return false;
}

async function processVideo(answerDetails: any, channelId: any, hindiBooks: any) {
    const isUploaded = await isYtUploaded(answerDetails.answer_id);
    if (!_.isEmpty(isUploaded)) {
        console.log("Video Already Uploaded");
        return;
    }
    const answerVideo = answerDetails.answer_video.slice(0, -4);
    const answerS3Url = `https://d3cvwyf9ksu0h5.cloudfront.net/${answerDetails.answer_video}`;
    // Step 1 (Download the answer video from s3)
    await download(answerS3Url, `./${answerVideo}.mp4`);
    // Step 2 (Convert the answer video to 720.mp4)
    let resolution = await doCommand(`ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of csv=s=x:p=0 "./${answerVideo}.mp4"`);
    resolution = resolution.toString().trim();
    if (!resCheck(resolution)) {
        await doCommand(`ffmpeg -y -i "${answerVideo}.mp4" -vf scale=1280:-2,setsar=1:1 -c:v libx264 -c:a copy "${answerVideo}_720.mp4"`);
    } else {
        await doCommand(`mv ${answerVideo}.mp4 ${answerVideo}_720.mp4`);
    }
    // Step 3 (Convert to MTS)
    await doCommand(`ffmpeg -i "${answerVideo}_720.mp4" -q 0 "${answerVideo}.MTS"`);
    // Step 4 (Concatenate the videos)
    const files = ["./intro_te.MTS", `${answerVideo}.MTS`, "./outro_te.MTS"];
    const fileList = files.map(x=>`-i ${x}`).join(" ");
    const filterComplex = files.map((x, i)=>`[${i}:v] [${i}:a]`).join(" ");
    await doCommand(`ffmpeg ${fileList} -filter_complex "${filterComplex} concat=n=${files.length}:v=1:a=1 [v] [a]" -map "[v]" -map "[a]" ${answerVideo}_output.MTS`);
    // Step 5 (Convert final MTS to mp4)
    await doCommand(`ffmpeg -i ${answerVideo}_output.MTS ${answerVideo}_output.mp4`);
    // Step 6 (Upload to YouTube)
    try {
        answerDetails.ocr_text = cleanText(answerDetails.ocr_text);
        const ytMeta = getYoutubeMeta(answerDetails, hindiBooks);
        const channelToken: any = await getChannelTokens(channelId);
        if (!channelToken.length) {
            return;
        }
        oAuth2Client.setCredentials({ ...channelToken[0] });
        await oAuth2Client.refreshAccessToken();
        google.options({ auth: oAuth2Client });
        const fileSize = fs.statSync(`${answerVideo}_output.mp4`).size;
        const params = {
            part: ["id", "snippet", "status"],
            requestBody: {
                snippet: {
                    title: ytMeta.title,
                    description: ytMeta.description,
                },
                status: {
                    privacyStatus: "private",
                    embeddable: false,
                    selfDeclaredMadeForKids: false,
                },
            },
            media: {
                mimeType: "video/mp4",
                body: fs.createReadStream(`${answerVideo}_output.mp4`),
            },
        };
        const res: any = await youtube.videos.insert(params, {
            onUploadProgress: evt => {
                const progress = (evt.bytesRead / fileSize) * 100;
                readline.clearLine(process.stdout, 0);
                readline.cursorTo(process.stdout, 0, null);
                process.stdout.write(`${Math.round(progress)}% complete`);
            },
        });
        if (res.data.id) {
            // insert in youtube_uploads table
            await setYoutubeUploads({
                question_id: answerDetails.question_id,
                answer_id: answerDetails.answer_id,
                answer_video: answerDetails.answer_video,
                youtube_id: res.data.id,
                yt_title : ytMeta.title,
                yt_description: ytMeta.description,
                privacy_status : "private",
            });
            // update in answers table
            await updateAnswerYoutubeIdByAnswerId(res.data.id, answerDetails.answer_id);
        }
    } catch (e) {
        console.log(e);
    }
    // Step 7 (Delete all the residual files)
    deleteFileArr([`./${answerVideo}.mp4`, `./${answerVideo}_720.mp4`, `./${answerVideo}.MTS`, `./${answerVideo}_output.MTS`, `./${answerVideo}_output.mp4`]);
}

export async function onMsg(msg: { data: any }[]) {
    for (let i = 0; i < msg.length; i++) {
        try {
            if (!fs.existsSync("./intro_te.MTS")) {
                const introS3Url = "https://d10lpgp6xz60nq.cloudfront.net/yt_intro_outro/telugu/Intro+Telugu.mp4";
                await download(introS3Url, "./intro_te.mp4");
                await doCommand("ffmpeg -i \"./intro_te.mp4\" -q 0 \"./intro_te.MTS\"");
            }
            if (!fs.existsSync("./outro_te.MTS")) {
                const outroS3Url = "https://d10lpgp6xz60nq.cloudfront.net/yt_intro_outro/telugu/Outro+Telugu.mp4";
                await download(outroS3Url, "./outro_te.mp4");
                await doCommand("ffmpeg -i \"./outro_te.mp4\" -q 0 \"./outro_te.MTS\"");
            }
            const channelId = "UCJPZ9OjWcF1UHqNPtUUOH_g";
            if (msg[i].data.answer.answer_video.includes(".mp4")) {
                await processVideo(msg[i].data.answer, channelId, msg[i].data.books);
            } else {
                console.log("skip");
            }
            deleteFileArr(["./intro_te.mp4", "./outro_te.mp4"]);
        } catch (e) {
            console.log(e);
        }
    }
}

export const opts = [{
    topic: "bull-cron.yt-answer.upload",
    fromBeginning: true,
    numberOfConcurrentPartitions: 1,
    autoCommitAfterNumberOfMessages: 1,
    autoCommitIntervalInMs: 899000,
}];
