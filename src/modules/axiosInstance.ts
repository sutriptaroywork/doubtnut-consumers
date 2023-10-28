import http from "http";
import https from "https";
import axios from "axios";

export const iasInstEsV7 = axios.create({
    httpAgent: new http.Agent({ keepAlive: true, maxSockets: 50 }),
    httpsAgent: new https.Agent({ keepAlive: true, maxSockets: 50 }),
});
