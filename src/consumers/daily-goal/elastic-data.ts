import { config } from "../../modules/config";
import { iasInstEsV7 } from "../../modules/axiosInstance";

interface FilterType {
    liveClass?: object;
    video?: object;
    pdf?: object;
}

interface UserContext {
    language: string;
}

interface Data {
    text: string;
    studentClass: number;
    count: number;
    version: string;
    contentAccess: number;
    filters: FilterType;
    userContext: UserContext;
}

export async function getDataForDailyGoal(obj: any) {
    try {
        const filter: FilterType = {
            liveClass: { subject: [obj.subject] },
        };
        const data: Data = {
            text: obj.chapter,
            studentClass: obj.class,
            count: 10,
            version: "v12.5",
            contentAccess: 0,
            filters: filter,
            userContext: {
                language: obj.locale,
            },
        };
        if (obj.type === "video") {
            data.filters = {
                video: { subject: [obj.subject] },
            };
            // options.data.count = 10;
        }
        if (obj.type === "pdf") {
            data.filters = {
                pdf: { subject: [obj.subject] },
            };
            // options.data.count = 10;
        }
        // let dataFromInApp = await axios(options);
        let dataFromInApp = await iasInstEsV7.get(`${config.iasVanillaBaseUrl}/api/v1/suggest`, { data, timeout: 1000 });
        dataFromInApp = dataFromInApp.data;
        return dataFromInApp;
    } catch (e) {
        return {};
    }
}
