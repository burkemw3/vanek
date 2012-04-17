package burkemw3.vanek;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;

public class Startup {
	private static class CliOptions {
		static String FORCE_UPLOAD = "force-upload";
	}
	public static void main(String[] arguments) throws IOException {
		AmazonS3 s3 = connectToS3();
		
		Options options = new Options();
		options.addOption("f", CliOptions.FORCE_UPLOAD,	false, "always upload");
		
		CommandLineParser parser = new GnuParser();
	    try {
	        // parse the command line arguments
	        CommandLine line = parser.parse(options, arguments);
	        boolean forceUpdate = line.hasOption(CliOptions.FORCE_UPLOAD);
	        
	        String[] remainingArguments = line.getArgs();
	        if (remainingArguments.length != 1) {
	        	throw new ParseException("Expecting a bucket name");
	        }
	        
	        String bucketName = remainingArguments[0];
	        
            ensureBucketExists(s3, bucketName);
	        
	        Set<File> files = getWorkingDirectoryJpegs();
	        
	        uploadFiles(s3, bucketName, files, forceUpdate);
			
	        createAndUploadZipFile(s3, bucketName, files);
	    } catch(ParseException exp) {
	        System.err.println("Parsing failed. Reason: " + exp.getMessage());
	    }
	}

	private static AmazonS3 connectToS3() throws FileNotFoundException,
			IOException {
		/* http://aws.amazon.com/security-credentials */
		String homeDirectoryPath = System.getProperty("user.home");
		String awsPropertiesPath = "AwsCredentials.properties";
		File awsCredentials = new File(homeDirectoryPath + "/" + awsPropertiesPath);
		FileInputStream fi = new FileInputStream(awsCredentials);
		AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(fi));
		return s3;
	}

	private static void ensureBucketExists(AmazonS3 s3, String bucketName) {
		if (false == s3.listBuckets().contains(bucketName)) {
			System.out.format("Creating bucket: %s\n", bucketName);
		    s3.createBucket(bucketName);
		}
		throw new RuntimeException("Grant everybody access");
	}
	
	private static Set<File> getWorkingDirectoryJpegs() {
		String path = System.getProperty("user.dir");
		File root = new File( path );
        Set<File> files = new HashSet<File>();
        
        for (File file : root.listFiles()) {
        	System.out.format("Looking at file %s: ", file.getName());
        	// TODO check for images, hidden, etc.
            if (false == file.isFile()) {
            	System.out.println("not a file");
                continue;
            }
            String filename = file.getName();
            if (false == filename.substring(filename.length()-3, filename.length()).equalsIgnoreCase("jpg")) {
            	System.out.println("not a jpg");
            	continue;
            }

        	System.out.println("adding");
            files.add(file);
        }
        
        return files;
	}
	
	private static void uploadFiles(AmazonS3 s3, String bucketName,
			Set<File> files, boolean overwriteExistingFiles) throws IOException {
        Set<String> existingKeys = new HashSet<String>();
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        ObjectListing objectListing = s3.listObjects(listObjectsRequest);
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        	existingKeys.add(objectSummary.getKey());
        }
        
        int total = files.size();
        int done = 0;
        int skipped = 0;
    	System.out.format("Uploading %d files\n", total);
        for (File file : files) {
    		System.out.format("Uploading %d of %d (%d already existed)\r", done+1, total, skipped);
            String key = file.getName();
        	if (false == overwriteExistingFiles && false == existingKeys.contains(key)) {
        		PutObjectRequest putRequest = new PutObjectRequest(bucketName, key, file);
        		putRequest.setStorageClass(StorageClass.ReducedRedundancy);
        		s3.putObject(putRequest);
        		++done;
        	} else {
        		++skipped;
        	}
        }
        System.out.print("\r");
	}

	private static void createAndUploadZipFile(AmazonS3 s3, String bucketName,
			Set<File> files) throws IOException {
		File zipFile = createZipFile(files);
		System.out.print("uploading zip...");
		s3.putObject(new PutObjectRequest(bucketName, zipFile.getName(), zipFile));
		System.out.println("done.");
		zipFile.delete();
	}
	
	static final int ZIP_BUFFER_SIZE = 2048;
	private static File createZipFile(Set<File> files) throws IOException {
		File file = new File("images.zip");
		FileOutputStream dest = new FileOutputStream(file);
		ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(dest));
		byte data[] = new byte[ZIP_BUFFER_SIZE];
        int total = files.size();
        int done = 0;
    	System.out.format("Zipping %d files\n", total);
		for (File inputFile : files) {
    		System.out.format("zipping %d of %d\r", done+1, total);
			FileInputStream fi = new FileInputStream(inputFile);
			BufferedInputStream origin = new BufferedInputStream(fi, ZIP_BUFFER_SIZE);
	    	ZipEntry entry = new ZipEntry(inputFile.getName());
	        out.putNextEntry(entry);
	        int count;
	        while((count = origin.read(data, 0, ZIP_BUFFER_SIZE)) != -1) {
	        	out.write(data, 0, count);
	        }
	        origin.close();
	        ++done;
	    }
		out.close();
        System.out.print("\r");
		
		return file;
	}
}

/* Other cool features
 * - multipart zip file upload
 * - upload smaller resizes files
 * - create html page to view and download
 * - generate sharing email
 * - delete an S3 directory
 * - schedule deletion of an S3 directory
 */
