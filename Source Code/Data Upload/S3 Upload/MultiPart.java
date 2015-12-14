import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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


public class MultiPart {

	private long progress;
	private long parts;
	private long partSize = 10*1024*1024; // Set part size to 5 MB.
	private List<PartETag> partETags;
	public static void main(String[] args) throws IOException {
		String existingBucketName = "awspdpmr";
		String keyName            = "project/samplem/reviews.json";
		String filePath           = "V:/reviews.json";  
		MultiPart run = new MultiPart();
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
		ExecutorService executor = Executors.newFixedThreadPool(5);//creating a pool of 5 threads  
		try {
			// Step 2: Upload parts.
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				// Last part can be less than 5 MB. Adjust part size.
				run.partSize = Math.min(run.partSize, (contentLength - filePosition));
				ps=System.currentTimeMillis();

				// Create request to upload a part.
				UploadPartRequest uploadRequest = new UploadPartRequest()
						.withBucketName(existingBucketName).withKey(keyName)
						.withUploadId(initResponse.getUploadId()).withPartNumber(i)
						.withFileOffset(filePosition)
						.withFile(file)
						.withPartSize(run.partSize);
				ps=System.currentTimeMillis();
				run.partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
				ps=System.currentTimeMillis()-ps;
				ps/=1000;
				rate = (double) run.partSize/1048576.0;
				rate/=(double) ps;
				System.out.println("Progress:"+((double) i/(double) run.parts)+"%"+ "took "+ps+"secs at "+rate+"mb/sec");
				filePosition += run.partSize;
			}
			//System.out.println("Dispatched all parts to threads");
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