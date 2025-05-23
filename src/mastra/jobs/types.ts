export interface JobResult {
  jobId: string;
  jobType: string;
  success: boolean;
  result?: any;
  error?: string;
}

export interface JobSubmission {
  jobType: string;
  arguments: any[];
}

export interface JobStatusNotifier {
  on(event: "result", listener: (result: JobResult) => void): this;
  removeListener(event: "result", listener: (result: JobResult) => void): this;
}
