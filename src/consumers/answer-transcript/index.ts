import {exec} from "child_process";
import speech from "@google-cloud/speech";
import { mysql, S3Helper} from "../../modules";
const fs = require("fs");
const {Storage} = require("@google-cloud/storage");


const s3 = new S3Helper();
const storage = new Storage();
const BUCKET = "doubtnut-qa-transcript";

// Creates a client
const client = new speech.v1p1beta1.SpeechClient();
async function setAnswerTranscript(obj) {
    const sql = "insert into answer_transcript (answer_id,transcript_duration_60) values(?,?) on duplicate key update transcript_duration_60 = ?";
    console.log(sql);
    // return mysql.writeCon.query(sql,[obj.answer_id, obj.transcript_duration_60, obj.transcript_duration_60]);
    return mysql.writeCon.query(sql, [obj.answer_id, obj.transcript_duration_60, obj.transcript_duration_60]);
}

async function computeTranscript(transcript)
{
    console.log(JSON.stringify(transcript.results));
    const final_60 = [];
    const transcript_json = JSON.stringify(transcript);

    transcript.results.forEach(result => {

        result.alternatives[0].words.forEach(wordInfo  => {
            let end_time: any;
            if (wordInfo.endTime.seconds)
            {
                if (wordInfo.endTime.nanos)
                {
                    // @ts-ignore
                    end_time = parseInt(wordInfo.endTime.seconds, 10) + (wordInfo.endTime.nanos / 100000000);
                }
                // @ts-ignore
                end_time = parseInt(wordInfo.endTime.seconds, 10);
            }
            else {
                end_time =  (wordInfo.endTime.nanos / 100000000);
            }

            // @ts-ignore
            const time_mod = parseInt(end_time / 60, 10);

            console.log(wordInfo.word, time_mod, end_time);
            if (final_60[time_mod] === undefined)
            {
                final_60[time_mod] = "";
            }

            final_60[time_mod] += wordInfo.word + " ";
        });
    });

    return final_60;
}

async function doExec(msg: any)
{


    try {

        /*
        If file exists in s3, do transcription
        else
            get answer video from s3, get mp3
            if mp3 does not exists in gs, upload
            get transcription
            save json to s3 and save data to table
         */
        console.log("msg: ", msg, typeof  msg);

        const answer_id = msg.answer_id;
        const answer_video = msg.answer_video;
        const answer_video_clean = msg.answer_video.replace(/\//g, "-");
        const upload_filename = answer_id + ".mp3";
        const input_video_file = answer_id;

        // check if json file exist, update only the table content
        const is_stored = await s3.checkObj("answer-transcript", answer_id);

        if (is_stored)
        {
            await s3.downloadS3Object("answer-transcript", answer_id, answer_id);
            const filedata = await fs.readFileSync(answer_id);
            const json_data = JSON.parse(filedata);
            console.log(json_data);
            const table_data = await computeTranscript(json_data);
            console.log("table_data", table_data);
            setAnswerTranscript({answer_id, transcript_duration_60: JSON.stringify(table_data)});
            fs.unlink(answer_id, err => {
                console.log(err);
            });
        }

        else {

            await s3.downloadS3Object("doubtnutteststreamin-hosting-mobilehub-1961518253", answer_video, answer_id);

            let command = "ffmpeg -i <input> <output> -y";

            command = command.replace("<input>", answer_id);
            command = command.replace("<output>", upload_filename);

            console.log(command);
            return new Promise((resolve, reject)=>{
                exec(command, async (err, stdout, stderr) => {
                    try {

                        if (err) {
                            console.log(err);
                            reject(1);
                        }
                    //  do transcript

                    const existInGS = await storage
                        .bucket(BUCKET)
                        .file(upload_filename)
                        .exists();
                    if (!existInGS[0])
                    {
                        await storage.bucket(BUCKET).upload(upload_filename, {
                            destination: upload_filename,
                        });

                    }

                    fs.unlink(upload_filename, error => {
                        console.log(error);
                    });

                    fs.unlink(answer_id, error => {
                        console.log(error);
                    });


                    const audio = {
                        uri: `gs://${BUCKET}/${upload_filename}`,
                        // content: fs.readFileSync(upload_filename).toString('base64'),
                    };
                    const config = {
                        enableWordTimeOffsets: true,
                        encoding: "MP3",
                        sampleRateHertz: 44000,
                        languageCode: "en-IN",
                        useEnhanced: true,
                    };
                    const request: any = {
                        audio,
                        config,
                    };

                    // Detects speech in the audio file
                    const [operation] = await client.longRunningRecognize(request);
                    const [response] = await operation.promise();
                    /*
                                    const [operation] = await client.longRunningRecognize(request);
                                    const [response] = await operation.promise();
                    */
                    console.log(response.results);

                    const table_data = await computeTranscript(response);

                    const transcript_json = JSON.stringify(response);

                    // @ts-ignore
                    await s3.uploadS3Object("answer-transcript", answer_id, Buffer.from(transcript_json, "utf-8"), "application/json");

                    await setAnswerTranscript({answer_id, transcript_duration_60: JSON.stringify(table_data)});
                    resolve(1);
                    }
                    catch (e) {
                        console.log(e);
                    reject(1);
                    }
                });

            });

        }

    } catch (e){
        console.log("Error 2", e);
    }

}

export async function onMsg(msg: any) {
    for (let i = 0; i < msg.length; i++) {

        try {
            await doExec(msg[i]);
        }
        catch (e)
        {
            console.log("Error", e);
        }
    }
}

export const opts = [{
    topic: "answer-transcript",
    fromBeginning: true,
    numberOfConcurrentPartitions: 4,
    autoCommitAfterNumberOfMessages: 1,
    autoCommitIntervalInMs: 60000
}];
