
import { Schema, model, connect } from 'mongoose';
import  { config } from '../../modules/config';
import { questionAskMongoDataModel} from './data_model';

const mongoDbConnectionUri = config.mongo.teslaFeed.database_url;

export async function onMsg(msg: {
    data: any,
    meta: any
}[]) {

  await connect(mongoDbConnectionUri);
  console.log('connnected to mongo db');

  try {
    for (let i = 0; i < msg.length; i++) {
      const { meta, data } = msg[i];
      const questionAskMongoDoc = new questionAskMongoDataModel(data);
      await questionAskMongoDoc.save();
      console.log("log saved ...");
    }
  } catch (E) {
    console.error(E);
  }  
}


export const opts = [
  {
    topic: "api-server.user-questions.log",
    fromBeginning: true,
    numberOfConcurrentPartitions: 1,
  },
];
