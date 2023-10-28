export interface ViserResponse {
  Blurscore: string;
  Diagram: string;
  DiagramScore: string;
  Language: string[];
  Mathscore: number;
  OCR: string;
  Orientation: string;
  Orientationscore: number;
  Printed: number;
  Printedscore: number;
  Score: number;
}

export interface QuestionData {
  student_id: string;
  qid: string;
  ocr_type: string;
  elastic_index?: string;
  iteration_name: string;
  is_match: string;
  question_image?: string;
  ocr?: string;
  user_locale?: string;
  relevance_score: object;
  tags: string[];
  viser_resp: ViserResponse;
  vision_ocr_android?: string;
  createdAt: string;
  updatedAt: string;
}
