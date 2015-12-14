import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.xspec.S;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;


public class MultiThreadedMultiPart {

	private long progress;
	private long parts;
	private long partSize = 1*1024*1024; // Set part size to 5 MB.
	private List<PartETag> partETags;
	class MultithreadedUpload implements Runnable {  
		private int partno;  
		private AmazonS3 s3Client;
		private UploadPartRequest uploadRequest;
		private long partsize;
		public MultithreadedUpload(int partno,UploadPartRequest uploadRequest,AmazonS3 s3Client,long partsize){  
			System.out.println("Starting thread "+partno+" "+Thread.currentThread().getName());
			this.partno=partno;  
			this.uploadRequest=uploadRequest;
			this.s3Client=s3Client;
			this.partsize=partsize;
		}  
		public void run() {  
			long ps=System.currentTimeMillis();
			partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
			progress++;
			ps=System.currentTimeMillis()-ps;
			ps/=1000;
			double rate = (double) partsize/1048576.0;
			rate/=(double) ps;
			System.out.println("completed upload of part no:"+partno+" by thread "+Thread.currentThread().getName());
			System.out.println("Progress:"+((double) progress/(double) parts)+"%"+ "took "+ps+"secs at "+rate+"mb/sec");
		}  
	} 
	public static void main(String[] args) throws IOException {
		String existingBucketName = "awspdpmr";
		String keyName            = "project/samplemm/reviews.json";
		String filePath           = "V:/reviews.json";  
		MultiThreadedMultiPart run = new MultiThreadedMultiPart();
		AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());        
		// Create a list of UploadPartResponse objects. You get one of these
		// for each part upload.
		run.partETags = new ArrayList<PartETag>();
		// Step 1: Initialize.
		InitiateMultipartUploadRequest initRequest = new 
				InitiateMultipartUploadRequest(existingBucketName, keyName);
		InitiateMultipartUploadResult initResponse = 
				s3Client.initiateMultipartUpload(initRequest);

		File file = new File(filePath);
		long contentLength = file.length();
		run.parts=(long) Math.ceil((double)contentLength/(double) run.partSize);
		System.out.println("File Length:"+contentLength);
		System.out.println("No of parts:"+run.parts);
		long ps=0,start=System.currentTimeMillis();
		double rate=0.0,mb=1024.0*1024.0;
		run.progress=0;
		
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);//creating a pool of 5 threads  
		//executor.s
		
		try {
			// Step 2: Upload parts.
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				// Last part can be less than 5 MB. Adjust part size.
				run.partSize = Math.min(run.partSize, (contentLength - filePosition));
				ps=System.currentTimeMillis();
				while((executor.getTaskCount()-executor.getCompletedTaskCount())>200)
                {
                	Thread.sleep(100000);
                }
				// Create request to upload a part.
				UploadPartRequest uploadRequest = new UploadPartRequest()
						.withBucketName(existingBucketName).withKey(keyName)
						.withUploadId(initResponse.getUploadId()).withPartNumber(i)
						.withFileOffset(filePosition)
						.withFile(file)
						.withPartSize(run.partSize);
				Runnable worker = run.new MultithreadedUpload(i, uploadRequest, new AmazonS3Client(new ProfileCredentialsProvider()), run.partSize);  
				executor.execute(worker);//calling execute method of ExecutorService  
				filePosition += run.partSize;
			}
			System.out.println("Dispatched all parts to threads");
			executor.shutdown();  
			while (!executor.isTerminated()) {   }  
			System.out.println("Upload Complete in "+((System.currentTimeMillis()-start)/1000)+" secs");
			System.out.println("Closing connection");
			start=System.currentTimeMillis();
			// Step 3: Complete.
			CompleteMultipartUploadRequest compRequest = new 
					CompleteMultipartUploadRequest(
							existingBucketName, 
							keyName, 
							initResponse.getUploadId(), 
							run.partETags);

			s3Client.completeMultipartUpload(compRequest);
		} catch (Exception e) {
			s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(
					existingBucketName, keyName, initResponse.getUploadId()));
		}
		System.out.println("connection closed in "+((System.currentTimeMillis()-start)/1000)+" secs");
	}
}