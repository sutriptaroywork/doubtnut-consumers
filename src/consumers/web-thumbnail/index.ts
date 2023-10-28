import mjAPI from "mathjax-node";
import puppeteer from "puppeteer";
import {S3Helper} from "../../modules";
const fs = require("fs");


const s3 = new S3Helper();
const BUCKET = "q-tn-web";

mjAPI.config({
    MathJax: {
        displayMessages: false,
        displayErrors: false,
        undefinedCharError: false,
        fontURL: "https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.5/fonts/HTML-CSS",
    },
});

mjAPI.start();

async function doExec(msg: any) {

    try {

        console.log("msg: ", msg, typeof msg);

        const question_id = msg.question_id;
        const ocr_text = msg.ocr_text;

        const image_name = question_id + "_web.png";
        // check if json file exist, update only the table content
        const is_stored = await s3.checkObj(BUCKET, image_name);
        if (false) {
            return;
        }

        await htmlRestructuring(ocr_text, image_name);

        const fileToUpload = await fs.readFileSync(image_name);
        // upload to s3
        await s3.uploadS3Object(BUCKET, image_name, Buffer.from(fileToUpload, "utf-8"), "image/png");

        // delete file
        fs.unlink(image_name, err => {
            console.log(err);
        });

    } catch (e) {
        console.log("Error 2", e);
    }

}

async function htmltopngconverter(htmltext, image_name) {
    try {
        const FileName = image_name;
        const browser = await puppeteer.launch({headless: true,
            args: ["--no-sandbox", "--disable-setuid-sandbox"]});
        const page = await browser.newPage();
        const pageHeight = await page.evaluate(() => document.body.scrollHeight);
        await page.setViewport({
            width: 1200,
            height: 677,
        });
        await page.setContent(htmltext);
        await page.screenshot({
            path: FileName,
        });
        await page.close();
        await browser.close();
    }
    catch (e)
    {
        console.error(e);
    }
}

async function ocrTohtml(yourMath) {

    return new Promise((resolve, reject) => {
        mjAPI.typeset({
            math: yourMath,
            format: "AsciiMath", // or "inline-TeX", "MathML"
            svg: true, // or svg:true, or html:true
        }, async function(data) {
            if (!data.errors) {
                resolve(data.svg);
            } else {
                reject(data.errors);
            }

        });
    });
}

async function htmlRestructuring(str, image_name) {
    try {
        let math = "";
        let start = false;
        let result = `<!DOCTYPE html>
        <html lang="en">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1.0">
          <title>Doubtnut Video</title>
          <link rel="preconnect" href="https://fonts.gstatic.com">
          <link href="https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700&display=swap" rel="stylesheet">
          <style>
            html, body {
              width: 100%;
              background-color: #ffffff;
              margin: 0px;
            }
            .video-wrap {
              height: 677px;
              width: 1200px;
              font-family: 'Lato', sans-serif;
              background-color: #000000;
              display: flex;
              flex-direction: column;
              align-items: center;
              text-align: center;
              position: relative;
            }
            .heading-img {
              height: 44px;
              margin: 16px 0px;
            }
            .video-heading {
              color: #ffffff;
              font-size: 34px;
              margin: 0px;
              flex-shrink: 0;
            }
            .video-text {
              height: 420px;
              overflow: hidden;
              color: #ffffff;
              font-size: 34px;
              line-height: 46px;
              vertical-align: center;
              margin: 90px 124px 0px 124px;
              flex-grow: 1;
            }
            .video-bottom {
              color: rgb(193, 84, 46);
              font-size: 24px;
              letter-spacing: 6px;
              margin: 26px;
              flex-shrink: 0;
            }
            .play-icon {
              height: 100%;
              width: 100%;
              display: flex;
              align-items: center;
              justify-content: center;
              position: absolute;
              z-index: 1;
              opacity:0.7;
            }
            .play-icon::after {
              content: '';
              border-top: 95px solid transparent;
              border-bottom: 95px solid transparent;
              border-right: 0px;
              border-left: 150px solid rgb(193, 84, 46);
              position: absolute;
              opacity: 0.8;
            }
          </style>
        </head>
        
        <body>
          <div class="video-wrap">
            <div class="play-icon"></div>
            <img class="heading-img" src="https://static.doubtnut.com/images/doubtnut_header_logo_white_new.svg">
            <p class="video-heading">FULL VIDEO AND TEXT SOLUTION</p>
            <p class="video-text">`;
        const endHtml = `</p><p class="video-bottom">WWW.DOUBTNUT.COM</p>
            </div>
          </body>
          
          </html>`;

        for (let i = 0; i < str.length; i++) {
            const char = str[i];
            // eslint-disable-next-line eqeqeq
            if (start == false && char == "`") {
                // ascii text started
                start = true;

                // eslint-disable-next-line eqeqeq
            } else if (start == true && char == "`") {
                // ascii text ended
                start = false;
                result = result + await ocrTohtml(math);
                // resultmath=resultmath+math;
                math = "";
            }

            // eslint-disable-next-line eqeqeq
            if (start == true && char != "`") {
                math = math + char;
                // eslint-disable-next-line eqeqeq
            } else if (start == false && char != "`") {
                result += char;
            }
            // eslint-disable-next-line eqeqeq
            if (i == str.length - 1) {
                result += endHtml;
            }
        }

        // console.log(result);

        await htmltopngconverter(result, image_name);

    }
    catch (e)
    {
        console.error(e);
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
    topic: "web.thumbnail.create",
    fromBeginning: true,
    numberOfConcurrentPartitions: 2,
}];
