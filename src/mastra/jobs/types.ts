export interface JobArgument {
  name: string;
  value: any;
}

export interface JobMessage {
  jobId: string;
  jobType: string;
  args: JobArgument[];
}

export interface JobResult {
  jobId: string;
  retval?: any;
  error?: string;
}

export interface JobSubmission {
  jobType: string;
  args: JobArgument[];
}

export interface JobStatusNotifier {
  on(event: "result", listener: (result: JobResult) => void): this;
  removeListener(event: "result", listener: (result: JobResult) => void): this;
}
