import { Schema, model, connect } from "mongoose";
import moment from "moment";

import {
    ViserResponse, 
    QuestionData
} from './interfaces';

const viserResponseSchema = new Schema<ViserResponse>({
  Blurscore: { type: String },
  Diagram: { type: String },
  DiagramScore: { type: String },
  Language: { type: [String] },
  Mathscore: { type: Number },
  OCR: { type: String },
  Orientation: { type: String },
  Orientationscore: { type: Number },
  Printed: { type: Number },
  Printedscore: { type: Number },
  Score: { type: Number },
});
const logSchema = new Schema<QuestionData>(
  {
    student_id: { type: String, index: true, required: true },
    qid: { type: String, index: true, required: true },
    ocr_type: { type: String, index: true, required: true },
    elastic_index: { type: String, default: "" },
    iteration_name: { type: String, index: true, default: null },
    is_match: { type: String },
    question_image: { type: String, default: null },
    ocr: { type: String, default: null },
    user_locale: { type: String, default: null },
    tags: { type: [], default: [] },
    viser_resp: { type: viserResponseSchema },
    vision_ocr_android: { type: String, default: null },
    relevance_score: { type: [] },
    createdAt: { type: String },
    updatedAt: { type: String },
  },
  {
    collection: "question_logs_user",
  }
);
logSchema.pre("save", function (next) {
  this.createdAt = moment(this.createdAt)
    .add(5, "h")
    .add(30, "m")
    .toISOString();
  this.updatedAt = moment(this.updatedAt)
    .add(5, "h")
    .add(30, "m")
    .toISOString();
  next();
});

export const questionAskMongoDataModel = model<QuestionData>(
  "question_logs_user",
  logSchema
);
